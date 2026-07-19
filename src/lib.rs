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

const SCAN_TIMEOUT: Duration = Duration::from_secs(20);
const SCAN_INTERVAL: Duration = Duration::from_millis(100);
const MAX_RECONNECTION_ATTEMPTS: u32 = 5;
const INITIAL_BACKOFF: Duration = Duration::from_secs(1);

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
        Some(DeviceCharacteristics { notify, battery })
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
        let enigo = Enigo::new(&Settings::default()).map_err(|error| {
            BleError::InteractionError(format!(
                "Failed to initialize keyboard emulator (Enigo): {}",
                error
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
    event_receiver: broadcast::Receiver<BleEvent>,
    config_receiver: watch::Receiver<BleConfig>,
    handler: Box<dyn InteractionHandler>,
}

impl InteractionManager {
    pub fn new(
        event_receiver: broadcast::Receiver<BleEvent>,
        config_receiver: watch::Receiver<BleConfig>,
        handler: Box<dyn InteractionHandler>,
    ) -> Self {
        Self {
            event_receiver,
            config_receiver,
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
                recv_result = self.event_receiver.recv() => {
                    match recv_result {
                        Ok(event) => {
                            if let BleEvent::KeyPress(direction) = event {
                                let result = match direction {
                                    ShiftDirection::Up => self.handler.shift_up().await,
                                    ShiftDirection::Down => self.handler.shift_down().await,
                                };
                                if let Err(error) = result {
                                    eprintln!("Interaction Error ({}): {}", direction, error);
                                }
                            }
                        }
                        Err(broadcast::error::RecvError::Lagged(_)) => {
                            eprintln!("Interaction Manager lagged: some events were missed");
                        }
                        Err(broadcast::error::RecvError::Closed) => {
                            eprintln!("Event channel closed; shutting down Interaction Manager");
                            break;
                        }
                    }
                }
                Ok(_) = self.config_receiver.changed() => {
                    // TODO: recreate handler when more handlers exists
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

/// A helper to encapsulate the standard setup of BLE components.
pub struct BleSystem {
    pub config_sender: watch::Sender<BleConfig>,
    pub config_receiver: watch::Receiver<BleConfig>,
    pub event_sender: broadcast::Sender<BleEvent>,
    pub event_receiver: broadcast::Receiver<BleEvent>,
    pub command_sender: mpsc::UnboundedSender<BleCommand>,
    pub command_receiver: mpsc::UnboundedReceiver<BleCommand>,
}

impl BleSystem {
    pub fn new(initial_config: BleConfig) -> Self {
        let (config_sender, config_receiver) = watch::channel(initial_config);
        let (event_sender, event_receiver) = broadcast::channel(16);
        let (command_sender, command_receiver) = mpsc::unbounded_channel();

        Self {
            config_sender,
            config_receiver,
            event_sender,
            event_receiver,
            command_sender,
            command_receiver,
        }
    }

    /// Boots the BLE worker and interaction manager as background tasks.
    pub async fn bootstrap<P: BleDeviceProfile + 'static>(
        self,
        profile: P,
        interaction_type: InteractionType,
    ) -> BleResult<(
        mpsc::UnboundedSender<BleCommand>,
        broadcast::Receiver<BleEvent>,
        watch::Sender<BleConfig>,
        watch::Receiver<BleConfig>,
    )> {
        let system = self;
        let config_receiver = system.config_receiver.clone();

        // 1. Spawn Worker
        let worker = BleWorker::new(
            profile,
            system.config_receiver.clone(),
            system.event_sender.clone(),
            system.command_receiver,
        )
        .await?;

        tokio::spawn(async move {
            if let Err(error) = worker.run().await {
                eprintln!("❌ BleWorker critical error: {}", error);
            }
        });

        // 2. Spawn Interaction Manager
        let interaction_manager = match interaction_type {
            InteractionType::Keyboard => InteractionManager::new_keyboard(
                system.event_receiver.resubscribe(),
                system.config_receiver.clone(),
            )?,
        };

        tokio::spawn(async move {
            interaction_manager.run().await;
        });

        Ok((
            system.command_sender,
            system.event_receiver,
            system.config_sender,
            config_receiver,
        ))
    }
}

pub enum InteractionType {
    Keyboard,
}

pub struct BleContext {
    profile: Arc<dyn BleDeviceProfile>,
    event_sender: broadcast::Sender<BleEvent>,
    config_receiver: watch::Receiver<BleConfig>,
}

impl BleContext {
    fn new(
        profile: Arc<dyn BleDeviceProfile>,
        event_sender: broadcast::Sender<BleEvent>,
        config_receiver: watch::Receiver<BleConfig>,
    ) -> Self {
        Self {
            profile,
            event_sender,
            config_receiver,
        }
    }

    async fn read_battery_level(&self, device: &BleDevice) -> BleResult<()> {
        match device.read_battery().await {
            Ok(Some(level)) => {
                let _ = self.event_sender.send(BleEvent::BatteryLevel(level));
                Ok(())
            }
            Ok(None) => Ok(()),
            Err(error) => {
                let _ = self.event_sender.send(BleEvent::Log(
                    format!("Battery read error: {}", error).into(),
                ));
                Err(error)
            }
        }
    }

    fn handle_notification(&self, device: &BleDevice, uuid: Uuid, value: &[u8]) {
        if let Some(event) = device.parse_notification(uuid, value) {
            if let BleEvent::KeyPress(direction) = event
                && self.config_receiver.borrow().debug
            {
                let _ = self
                    .event_sender
                    .send(BleEvent::Log(format!("Pressed: {}", direction).into()));
            }
            let _ = self.event_sender.send(event);
        }
    }

    async fn setup_device(&self, device: &mut BleDevice) -> BleResult<()> {
        device.connect_and_subscribe().await?;
        let _ = self.read_battery_level(device).await;
        Ok(())
    }

    async fn process_notifications(&self, device: &mut BleDevice) -> BleResult<()> {
        let device_clone = device.clone();
        device
            .listen(move |notification| {
                self.handle_notification(&device_clone, notification.uuid, &notification.value);
            })
            .await
    }

    async fn handle_peripheral(self: Arc<Self>, mut device: BleDevice) -> BleResult<()> {
        self.setup_device(&mut device).await?;

        let device_for_poll = device.clone();
        let context_for_poll = self.clone();
        tokio::spawn(async move {
            let mut poll_interval = tokio::time::interval(Duration::from_secs(300));
            loop {
                poll_interval.tick().await;
                if context_for_poll
                    .read_battery_level(&device_for_poll)
                    .await
                    .is_err()
                {
                    break;
                }
            }
        });

        self.process_notifications(&mut device).await
    }
}

enum InternalEvent {
    DeviceFound(BleDevice),
    StatusUpdate(State),
    Log(Cow<'static, str>),
}

pub struct BleWorker {
    context: Arc<BleContext>,
    command_receiver: mpsc::UnboundedReceiver<BleCommand>,
    adapter: btleplug::platform::Adapter,
    worker_event_sender: mpsc::UnboundedSender<InternalEvent>,
    worker_event_receiver: mpsc::UnboundedReceiver<InternalEvent>,
}

impl BleWorker {
    pub async fn new<P: BleDeviceProfile + 'static>(
        profile: P,
        config_receiver: watch::Receiver<BleConfig>,
        event_sender: broadcast::Sender<BleEvent>,
        command_receiver: mpsc::UnboundedReceiver<BleCommand>,
    ) -> BleResult<Self> {
        let adapter = PlatformManager::new()
            .await?
            .adapters()
            .await?
            .into_iter()
            .next()
            .ok_or(BleError::NoAdapterAvailable)?;
        let (worker_event_sender, worker_event_receiver) = mpsc::unbounded_channel();
        let context = Arc::new(BleContext::new(
            Arc::new(profile),
            event_sender,
            config_receiver,
        ));
        Ok(Self {
            context,
            command_receiver,
            adapter,
            worker_event_sender,
            worker_event_receiver,
        })
    }

    async fn execute_scan(
        adapter: btleplug::platform::Adapter,
        profile: Arc<dyn BleDeviceProfile>,
        worker_event_sender: mpsc::UnboundedSender<InternalEvent>,
    ) {
        if let Err(error) = adapter.start_scan(ScanFilter::default()).await {
            let _ = worker_event_sender.send(InternalEvent::Log(format!("Scan error: {}", error)));
            return;
        }

        let mut events = match adapter.events().await {
            Ok(e) => e,
            Err(error) => {
                let _ = worker_event_sender.send(InternalEvent::Log(format!(
                    "Failed to get adapter events: {}",
                    error
                )));
                return;
            }
        };

        let start_time = std::time::Instant::now();

        loop {
            tokio::select! {
                Some(event) = events.next() => {
                    if let btleplug::api::CentralEvent::DeviceDiscovered(peripheral_id) = event {
                        let device = async {
                            let peripheral = adapter.peripheral(&peripheral_id).await.ok()?;
                            let properties = peripheral.properties().await.ok()??;
                            if profile.matches(properties.local_name.as_deref()) {
                                Some(BleDevice::new(profile.clone(), peripheral))
                            } else {
                                None
                            }
                        }.await;

                        if let Some(found_device) = device {
                            let _ = worker_event_sender.send(InternalEvent::DeviceFound(found_device));
                            return;
                        }
                    }
                }
                _ = tokio::time::sleep(SCAN_INTERVAL) => {
                    if start_time.elapsed() >= SCAN_TIMEOUT {
                        break;
                    }
                }
            }
        }
        let _ = worker_event_sender.send(InternalEvent::StatusUpdate(State::Disconnected));
    }

    async fn start_connection_session(&self, device: BleDevice) {
        let addr: Cow<'static, str> = Cow::Owned(device.address().to_string());
        let context = self.context.clone();
        let event_sender = context.event_sender.clone();

        tokio::spawn(async move {
            let mut retry_count = 0;
            let mut backoff_duration = INITIAL_BACKOFF;

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
                let _ = event_sender.send(BleEvent::StatusUpdate(status));

                match context.clone().handle_peripheral(device.clone()).await {
                    Ok(_) => break,
                    Err(error) => {
                        let _ = event_sender.send(BleEvent::Log(format!(
                            "Connection lost: {}. Retrying...",
                            error
                        )));
                        retry_count += 1;
                        if retry_count > MAX_RECONNECTION_ATTEMPTS {
                            break;
                        }
                        tokio::time::sleep(backoff_duration).await;
                        backoff_duration *= 2;
                    }
                }
            }
            let _ = event_sender.send(BleEvent::StatusUpdate(State::Disconnected));
        });
    }

    pub async fn run(mut self) -> BleResult<()> {
        let mut scan_task: Option<tokio::task::JoinHandle<()>> = None;
        let mut connection_task: Option<tokio::task::JoinHandle<()>> = None;

        loop {
            tokio::select! {
                Some(command) = self.command_receiver.recv() => {
                    if let Some(task) = scan_task.take() { task.abort(); }
                    if let Some(task) = connection_task.take() { task.abort(); }

                    match command {
                        BleCommand::SearchDevice => {
                            let _ = self.context.event_sender.send(BleEvent::StatusUpdate(State::Searching));
                            scan_task = Some(tokio::spawn(Self::execute_scan(self.adapter.clone(), self.context.profile.clone(), self.worker_event_sender.clone())));
                        }
                        BleCommand::StopSearch => {
                            let _ = self.context.event_sender.send(BleEvent::StatusUpdate(State::Disconnected));
                        }
                    }
                }
                Some(internal_event) = self.worker_event_receiver.recv() => match internal_event {
                    InternalEvent::DeviceFound(device) => self.start_connection_session(device).await,
                    InternalEvent::StatusUpdate(status) => { let _ = self.context.event_sender.send(BleEvent::StatusUpdate(status)); }
                    InternalEvent::Log(message) => { let _ = self.context.event_sender.send(BleEvent::Log(message)); }
                }
            }
        }
    }
}
