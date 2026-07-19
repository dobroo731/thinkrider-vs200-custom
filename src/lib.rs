use async_trait::async_trait;
use btleplug::api::{Central, Characteristic, Manager, Peripheral, ScanFilter, ValueNotification};
use btleplug::platform::{Manager as PlatformManager, Peripheral as PlatformPeripheral};
use enigo::{Direction, Enigo, Key, Keyboard, Settings};
use futures::stream::StreamExt;
use std::borrow::Cow;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::{broadcast, mpsc, watch};
use uuid::Uuid;

#[derive(thiserror::Error, Debug)]
pub enum BleError {
    #[error("BLE Adapter Error: {0}")]
    AdapterError(#[from] btleplug::Error),

    #[error("Interaction Error: {0}")]
    InteractionError(String),

    #[error("Target BLE device not found")]
    DeviceNotFound,

    #[error("Connection failed: {0}")]
    ConnectionFailed(String),

    #[error("No BLE adapter available on this system")]
    NoAdapterAvailable,

    #[error("Generic error: {0}")]
    Generic(#[from] Box<dyn std::error::Error + Send + Sync>),
}

type BleResult<T> = Result<T, BleError>;

#[derive(Clone)]
pub struct DeviceCharacteristics {
    pub notify: Characteristic,
    pub battery: Option<Characteristic>,
}

pub trait BleDeviceProfile: Send + Sync {
    fn name_filter(&self) -> &'static str;
    fn notify_uuid(&self) -> Uuid;
    fn battery_uuid(&self) -> Uuid;
    fn shift_up_pattern(&self) -> &'static [u8];
    fn shift_down_pattern(&self) -> &'static [u8];

    /// Maps available characteristics to those required by this profile.
    fn find_characteristics(
        &self,
        characteristics: &[Characteristic],
    ) -> Option<DeviceCharacteristics> {
        let mut notify = None;
        let mut battery = None;

        for characteristic in characteristics {
            if characteristic.uuid == self.notify_uuid() {
                notify = Some(characteristic.clone());
            } else if characteristic.uuid == self.battery_uuid() {
                battery = Some(characteristic.clone());
            }
        }

        let notify = notify?;
        Some(DeviceCharacteristics {
            notify,
            battery,
        })
    }

    fn parse_notification(&self, uuid: Uuid, value: &[u8]) -> Option<BleEvent> {
        if uuid == self.battery_uuid() {
            return self.parse_battery_level(value).map(BleEvent::BatteryLevel);
        }
        if uuid == self.notify_uuid() {
            if value == self.shift_up_pattern() {
                return Some(BleEvent::KeyPress(ShiftDirection::Up));
            }
            if value == self.shift_down_pattern() {
                return Some(BleEvent::KeyPress(ShiftDirection::Down));
            }
        }
        None
    }

    fn parse_battery_level(&self, data: &[u8]) -> Option<u8> {
        data.first().copied()
    }

    fn matches(&self, name: Option<&str>) -> bool {
        name.is_some_and(|n| n.contains(self.name_filter()))
    }
}

/// Represents an active connection to a BLE device.
#[derive(Clone)]
pub struct BleDevice {
    profile: Arc<dyn BleDeviceProfile>,
    peripheral: PlatformPeripheral,
    address: Cow<'static, str>,
    battery: Option<Characteristic>,
}

impl BleDevice {
    pub fn new(profile: Arc<dyn BleDeviceProfile>, peripheral: PlatformPeripheral) -> Self {
        let address = peripheral.address().to_string();
        Self {
            address: Cow::Owned(address),
            profile,
            peripheral,
            battery: None,
        }
    }

    pub async fn find(
        adapter: &btleplug::platform::Adapter,
        profile: Arc<dyn BleDeviceProfile>,
    ) -> BleResult<Self> {
        let peripherals = adapter.peripherals().await?;
        for peripheral in peripherals {
            if let Some(props) = peripheral.properties().await?
                && profile.matches(props.local_name.as_deref())
            {
                return Ok(Self::new(profile, peripheral));
            }
        }
        Err(BleError::DeviceNotFound)
    }

    pub fn address(&self) -> &str {
        &self.address
    }

    pub async fn listen<F>(&self, mut handler: F) -> BleResult<()>
    where
        F: FnMut(ValueNotification) + Send,
    {
        let mut stream = self.peripheral.notifications().await?;
        while let Some(notification) = stream.next().await {
            handler(notification);
        }
        Ok(())
    }

    pub async fn connect_and_subscribe(&mut self) -> BleResult<()> {
        self.peripheral.connect().await?;
        self.peripheral.discover_services().await?;

        let chars = self.peripheral.characteristics();
        // Convert BTreeSet to Vec for the trait method
        let chars_vec: Vec<Characteristic> = chars.into_iter().collect();
        let device_chars =
            self.profile
                .find_characteristics(&chars_vec)
                .ok_or(BleError::ConnectionFailed(
                    "Could not find required characteristics for this device".into(),
                ))?;

        // Subscribe to notifications
        self.peripheral.subscribe(&device_chars.notify).await?;

        // Optional battery subscription/storage
        if let Some(battery_characteristic) = device_chars.battery
            && self
                .peripheral
                .subscribe(&battery_characteristic)
                .await
                .is_ok()
        {
            self.battery = Some(battery_characteristic);
        }

        Ok(())
    }

    pub async fn read_battery(&self) -> BleResult<Option<u8>> {
        if let Some(battery_characteristic) = &self.battery {
            let value = self.peripheral.read(battery_characteristic).await?;
            return Ok(self.profile.parse_battery_level(&value));
        }
        Ok(None)
    }

    pub fn parse_notification(&self, uuid: Uuid, value: &[u8]) -> Option<BleEvent> {
        self.profile.parse_notification(uuid, value)
    }
}

/// Zero Sized Type for the ThinkRider VS01/200 profile.
pub struct ThinkRiderVS200;

#[async_trait]
impl BleDeviceProfile for ThinkRiderVS200 {
    fn name_filter(&self) -> &'static str {
        "Think VS01-"
    }

    fn notify_uuid(&self) -> Uuid {
        uuid::uuid!("0000fea1-0000-1000-8000-00805f9b34fb")
    }

    fn battery_uuid(&self) -> Uuid {
        uuid::uuid!("00002a19-0000-1000-8000-00805f9b34fb")
    }

    fn shift_up_pattern(&self) -> &'static [u8] {
        &[0xf3, 0x05, 0x03, 0x01, 0xfc]
    }

    fn shift_down_pattern(&self) -> &'static [u8] {
        &[0xf3, 0x05, 0x03, 0x00, 0xfb]
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, strum::Display)]
pub enum ShiftDirection {
    Up,
    Down,
}

#[async_trait]
pub trait InteractionHandler: Send + Sync {
    async fn shift_up(&mut self) -> BleResult<()>;
    async fn shift_down(&mut self) -> BleResult<()>;
}

pub struct KeyboardInteraction {
    enigo: Enigo,
    up_key: Key,
    down_key: Key,
}

impl KeyboardInteraction {
    pub fn new(config: &BleConfig) -> BleResult<Self> {
        let enigo = Enigo::new(&Settings::default()).map_err(|e| {
            BleError::InteractionError(format!(
                "Failed to initialize keyboard emulator (Enigo): {}",
                e
            ))
        })?;
        Ok(Self {
            enigo,
            up_key: config.up_key,
            down_key: config.down_key,
        })
    }
}

#[async_trait]
impl InteractionHandler for KeyboardInteraction {
    async fn shift_up(&mut self) -> BleResult<()> {
        self.enigo
            .key(self.up_key, Direction::Press)
            .map_err(|e| BleError::InteractionError(e.to_string()))?;
        self.enigo
            .key(self.up_key, Direction::Release)
            .map_err(|e| BleError::InteractionError(e.to_string()))?;
        Ok(())
    }

    async fn shift_down(&mut self) -> BleResult<()> {
        self.enigo
            .key(self.down_key, Direction::Press)
            .map_err(|e| BleError::InteractionError(e.to_string()))?;
        self.enigo
            .key(self.down_key, Direction::Release)
            .map_err(|e| BleError::InteractionError(e.to_string()))?;
        Ok(())
    }
}

pub struct InteractionManager {
    event_rx: broadcast::Receiver<BleEvent>,
    config_rx: watch::Receiver<BleConfig>,
    handler: Box<dyn InteractionHandler>,
}

impl InteractionManager {
    pub fn new(
        event_rx: broadcast::Receiver<BleEvent>,
        config_rx: watch::Receiver<BleConfig>,
        handler: Box<dyn InteractionHandler>,
    ) -> Self {
        Self {
            event_rx,
            config_rx,
            handler,
        }
    }

    pub fn new_keyboard(
        event_receiver: broadcast::Receiver<BleEvent>,
        config_receiver: watch::Receiver<BleConfig>,
    ) -> BleResult<Self> {
        let handler = Box::new(KeyboardInteraction::new(&config_receiver.borrow())?);
        Ok(Self::new(event_receiver, config_receiver, handler))
    }

    pub async fn run(mut self) {
        loop {
            tokio::select! {
                Ok(event) = self.event_rx.recv() => {
                    if let BleEvent::KeyPress(direction) = event {
                        let res = match direction {
                            ShiftDirection::Up => self.handler.shift_up().await,
                            ShiftDirection::Down => self.handler.shift_down().await,
                        };
                        if let Err(e) = res {
                            eprintln!("Interaction Error ({}): {}", direction, e);
                        }
                    }
                }
                Ok(_) = self.config_rx.changed() => {
                    let config = self.config_rx.borrow();
                        match KeyboardInteraction::new(&config) {
                            Ok(new_handler) => {
                                self.handler = Box::new(new_handler);
                                eprintln!("⚙️  Interaction handler recreated due to keymapping change");
                            }
                            Err(e) => eprintln!("❌ Failed to recreate interaction handler: {}", e),
                    }
                }
            }
        }
    }
}

#[derive(Debug, Clone)]
pub enum State {
    Disconnected,
    Searching,
    Connecting { device_address: Cow<'static, str> },
    Connected { device_address: Cow<'static, str> },
    Reconnecting { device_address: Cow<'static, str> },
}

impl std::fmt::Display for State {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            State::Disconnected => write!(f, "Disconnected"),
            State::Searching => write!(f, "Scanning..."),
            State::Connecting { device_address } => {
                write!(f, "Connecting to {}...", device_address)
            }
            State::Connected { device_address } => write!(f, "Connected to {}", device_address),
            State::Reconnecting { device_address } => {
                write!(f, "Reconnecting to {}...", device_address)
            }
        }
    }
}

#[derive(Debug, Clone)]
pub enum BleEvent {
    StatusUpdate(State),
    Log(Cow<'static, str>),
    BatteryLevel(u8),
    KeyPress(ShiftDirection),
}

#[derive(Debug, Clone)]
pub enum BleCommand {
    SearchDevice,
    StopSearch,
}

#[derive(Debug, Clone)]
pub struct BleConfig {
    pub up_key: Key,
    pub down_key: Key,
    pub debug: bool,
    pub notifications_enabled: bool,
}

impl Default for BleConfig {
    fn default() -> Self {
        Self {
            up_key: Key::K,
            down_key: Key::I,
            debug: false,
            notifications_enabled: true,
        }
    }
}

pub struct BleContext {
    profile: Arc<dyn BleDeviceProfile>,
    tx_event: broadcast::Sender<BleEvent>,
    rx_config: watch::Receiver<BleConfig>,
}

impl BleContext {
    fn new(
        profile: Arc<dyn BleDeviceProfile>,
        tx_event: broadcast::Sender<BleEvent>,
        rx_config: watch::Receiver<BleConfig>,
    ) -> Self {
        Self {
            profile,
            tx_event,
            rx_config,
        }
    }

    async fn read_battery_level(&self, device: &BleDevice) -> BleResult<()> {
        match device.read_battery().await {
            Ok(Some(lvl)) => {
                let _ = self.tx_event.send(BleEvent::BatteryLevel(lvl));
                Ok(())
            }
            Ok(None) => Ok(()),
            Err(e) => {
                let _ = self
                    .tx_event
                    .send(BleEvent::Log(format!("Battery read error: {}", e).into()));
                Err(e)
            }
        }
    }

    fn handle_notification(&self, device: &BleDevice, uuid: Uuid, value: &[u8]) {
        if let Some(event) = device.parse_notification(uuid, value) {
            if let BleEvent::KeyPress(dir) = event
                && self.rx_config.borrow().debug
            {
                let _ = self
                    .tx_event
                    .send(BleEvent::Log(format!("Pressed: {}", dir).into()));
            }
            let _ = self.tx_event.send(event);
        }
    }

    async fn setup_device(&self, device: &mut BleDevice) -> BleResult<()> {
        device.connect_and_subscribe().await?;
        let _ = self.read_battery_level(device).await;
        Ok(())
    }

    async fn process_notifications(&self, device: &mut BleDevice) -> BleResult<()> {
        let dev = device.clone();
        device
            .listen(move |n| {
                self.handle_notification(&dev, n.uuid, &n.value);
            })
            .await
    }

    async fn handle_peripheral(self: Arc<Self>, mut device: BleDevice) -> BleResult<()> {
        self.setup_device(&mut device).await?;

        loop {
            tokio::select! {
                maybe_notification = notifications.next() => {
                    match maybe_notification {
                        Some(n) => self.handle_notification(n.uuid, &n.value).await,
                        None => {
                            let _ = self.tx_event.send(BleEvent::Log("BLE notification stream closed".into()));
                            return Err("Notification stream terminated".into());
                        }
                    }
                }
                _ = battery_poll_interval.tick() => {
                    self.read_battery_level(peripheral, battery_char).await;
                }
            }
        }
    }

        self.process_notifications(&mut device).await
    }
}

enum InternalEvent {
    DeviceFound(BleDevice),
    StatusUpdate(State),
    Log(Cow<'static, str>),
}

pub struct BleWorker {
    ctx: Arc<BleContext>,
    rx_cmd: mpsc::UnboundedReceiver<BleCommand>,
    adapter: btleplug::platform::Adapter,
    internal_tx: mpsc::UnboundedSender<InternalEvent>,
    internal_rx: mpsc::UnboundedReceiver<InternalEvent>,
}

impl BleWorker {
    pub async fn new<P: BleDeviceProfile + 'static>(
        profile: P,
        rx_config: watch::Receiver<BleConfig>,
        tx_event: broadcast::Sender<BleEvent>,
        rx_cmd: mpsc::UnboundedReceiver<BleCommand>,
    ) -> BleResult<Self> {
        let adapter = PlatformManager::new()
            .await?
            .adapters()
            .await?
            .into_iter()
            .next()
            .ok_or(BleError::NoAdapterAvailable)?;
        let (internal_tx, internal_rx) = mpsc::unbounded_channel();
        let ctx = Arc::new(BleContext::new(Arc::new(profile), tx_event, rx_config));
        Ok(Self {
            ctx,
            rx_cmd,
            adapter,
            internal_tx,
            internal_rx,
        })
    }

    async fn execute_scan(
        adapter: btleplug::platform::Adapter,
        profile: Arc<dyn BleDeviceProfile>,
        tx: mpsc::UnboundedSender<InternalEvent>,
    ) {
        if let Err(e) = adapter.start_scan(ScanFilter::default()).await {
            let _ = tx.send(InternalEvent::Log(format!("Scan error: {}", e).into()));
            return;
        }

        let mut events = match adapter.events().await {
            Ok(e) => e,
            Err(e) => {
                let _ = tx.send(InternalEvent::Log(
                    format!("Failed to get adapter events: {}", e).into(),
                ));
                return;
            }
        };

        let start = std::time::Instant::now();
        let total_timeout = Duration::from_secs(20);

        loop {
            tokio::select! {
                Some(event) = events.next() => {
                    if let btleplug::api::CentralEvent::DeviceDiscovered(p_id) = event {
                        let device = async {
                            let p = adapter.peripheral(&p_id).await.ok()?;
                            let props = p.properties().await.ok()??;
                            if profile.matches(props.local_name.as_deref()) {
                                Some(BleDevice::new(profile.clone(), p))
                            } else {
                                None
                            }
                        }.await;

                        if let Some(d) = device {
                            let _ = tx.send(InternalEvent::DeviceFound(d));
                            return;
                        }
                    }
                }
                _ = tokio::time::sleep(Duration::from_millis(100)) => {
                    if start.elapsed() >= total_timeout {
                        break;
                    }
                }
            }
        }
        let _ = tx.send(InternalEvent::StatusUpdate(State::Disconnected));
    }

    async fn start_connection_session(&self, device: BleDevice) {
        let addr: Cow<'static, str> = Cow::Owned(device.address().to_string());
        let ctx = self.ctx.clone();
        let tx_event = ctx.tx_event.clone();

        tokio::spawn(async move {
            let mut retry_count = 0;
            let mut backoff = Duration::from_secs(1);

            loop {
                let status = if retry_count > 0 {
                    State::Reconnecting {
                        device_address: addr.clone(),
                    }
                } else {
                    State::Connected {
                        device_address: addr.clone(),
                    }
                };
                let _ = tx_event.send(BleEvent::StatusUpdate(status));

                match ctx.clone().handle_peripheral(peripheral.clone()).await {
                    Ok(_) => {
                        let _ = tx_event
                            .send(BleEvent::Log("Peripheral session ended gracefully".into()));
                        break;
                    }
                    Err(e) => {
                        let _ = tx_event.send(BleEvent::Log(
                            format!(
                                "Connection lost: {}. Retrying... ({} / {})",
                                e,
                                retry_count + 1,
                                max_retries
                            )
                            .into(),
                        ));
                        retry_count += 1;
                        if retry_count > max_retries {
                            let _ = tx_event.send(BleEvent::Log(
                                "Max reconnection attempts reached. Giving up.".into(),
                            ));
                            break;
                        }
                        tokio::time::sleep(backoff).await;
                        backoff *= 2;
                    }
                }
            }
            let _ = tx_event.send(BleEvent::StatusUpdate(State::Disconnected));
        });
    }

    pub async fn run(mut self) -> BleResult<()> {
        let mut scan_task: Option<tokio::task::JoinHandle<()>> = None;
        let mut connection_task: Option<tokio::task::JoinHandle<()>> = None;

        let stop_tasks = |scan: &mut Option<tokio::task::JoinHandle<()>>,
                          conn: &mut Option<tokio::task::JoinHandle<()>>| {
            if let Some(t) = scan.take() {
                t.abort();
            }
            if let Some(t) = conn.take() {
                t.abort();
            }
        };

        loop {
            tokio::select! {
                Some(cmd) = self.rx_cmd.recv() => {
                    match cmd {
                        BleCommand::SearchDevice => {
                            stop_tasks(&mut scan_task, &mut connection_task);

                            if self.ctx.rx_config.borrow().notifications_enabled {
                                let _ = self.ctx.tx_event.send(BleEvent::Log("User triggered 'Search Device'".into()));
                            }
                            let _ = self.ctx.tx_event.send(BleEvent::StatusUpdate(State::Searching));

                            let adapter = self.adapter.clone();
                            let internal_tx = self.internal_tx.clone();
                            scan_task = Some(tokio::spawn(Self::execute_scan(adapter, internal_tx)));
                        }
                        BleCommand::StopSearch => {
                            stop_tasks(&mut scan_task, &mut connection_task);
                            let _ = self.ctx.tx_event.send(BleEvent::StatusUpdate(State::Disconnected));
                            let _ = self.ctx.tx_event.send(BleEvent::Log("Search stopped by user".into()));
                        }
                    }
                }
                Some(internal_event) = self.internal_rx.recv() => {
                    match internal_event {
                        InternalEvent::DeviceFound(peripheral) => {
                            self.start_connection_session(peripheral).await;
                        }
                        InternalEvent::StatusUpdate(s) => {
                            let _ = self.ctx.tx_event.send(BleEvent::StatusUpdate(s));
                        }
                        InternalEvent::Log(msg) => {
                            let _ = self.ctx.tx_event.send(BleEvent::Log(msg));
                        }
                    }
                }
            }
        }
    }
}
