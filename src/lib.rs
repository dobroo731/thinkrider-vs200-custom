use btleplug::api::{Central, Manager, Peripheral, ScanFilter};
use btleplug::platform::{Manager as PlatformManager, Peripheral as PlatformPeripheral};
use enigo::{Direction, Enigo, Key, Keyboard, Settings};
use futures::stream::StreamExt;
use std::borrow::Cow;
use std::error::Error;
use std::sync::{Arc, OnceLock};
use tokio::sync::{broadcast, mpsc, watch};
use uuid::Uuid;

pub const DEVICE_NAME_FILTER: &str = "Think VS01-";
pub const NOTIFY_UUID_STR: &str = "0000fea1-0000-1000-8000-00805f9b34fb";
pub const BATTERY_UUID_STR: &str = "00002a19-0000-1000-8000-00805f9b34fb";
pub const SHIFT_UP_PATTERN: &[u8] = &[0xf3, 0x05, 0x03, 0x01, 0xfc];
pub const SHIFT_DOWN_PATTERN: &[u8] = &[0xf3, 0x05, 0x03, 0x00, 0xfb];

static NOTIFY_UUID: OnceLock<Uuid> = OnceLock::new();
static BATTERY_UUID: OnceLock<Uuid> = OnceLock::new();

fn get_notify_uuid() -> Uuid {
    *NOTIFY_UUID.get_or_init(|| Uuid::parse_str(NOTIFY_UUID_STR).expect("Invalid NOTIFY_UUID_STR"))
}

fn get_battery_uuid() -> Uuid {
    *BATTERY_UUID
        .get_or_init(|| Uuid::parse_str(BATTERY_UUID_STR).expect("Invalid BATTERY_UUID_STR"))
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
    KeyPress(Cow<'static, str>),
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

/// Shared context for the BLE worker and its spawned tasks.
struct BleContext {
    tx_event: broadcast::Sender<BleEvent>,
    rx_config: watch::Receiver<BleConfig>,
}

impl BleContext {
    fn new(tx_event: broadcast::Sender<BleEvent>, rx_config: watch::Receiver<BleConfig>) -> Self {
        Self {
            tx_event,
            rx_config,
        }
    }

    async fn read_battery_level(
        &self,
        peripheral: &PlatformPeripheral,
        characteristic: &Option<btleplug::api::Characteristic>,
    ) {
        if let Some(bc) = characteristic
            && let Ok(val) = peripheral.read(bc).await
            && let Some(&level) = val.first()
        {
            let _ = self.tx_event.send(BleEvent::BatteryLevel(level));
        }
    }

    async fn handle_keystroke(&self, key_name: &'static str, key_enum: Key, enigo: &mut Enigo) {
        let config = self.rx_config.borrow();
        if config.debug {
            let _ = self
                .tx_event
                .send(BleEvent::Log(format!("Pressed: {}", key_name).into()));
        }
        let _ = self.tx_event.send(BleEvent::KeyPress(key_name.into()));
        let _ = enigo.key(key_enum, Direction::Press);
        let _ = enigo.key(key_enum, Direction::Release);
    }

    fn find_characteristic<'a>(
        &self,
        chars: &'a std::collections::BTreeSet<btleplug::api::Characteristic>,
        uuid: Uuid,
    ) -> Option<&'a btleplug::api::Characteristic> {
        chars.iter().find(|c| c.uuid == uuid)
    }

    async fn handle_notification(&self, uuid: Uuid, value: &[u8], enigo: &mut Enigo) {
        let battery_uuid = get_battery_uuid();
        if uuid == battery_uuid {
            if let Some(&level) = value.first() {
                let _ = self.tx_event.send(BleEvent::BatteryLevel(level));
            }
            return;
        }

        let config = self.rx_config.borrow().clone();
        match value {
            SHIFT_UP_PATTERN => self.handle_keystroke("Up", config.up_key, enigo).await,
            SHIFT_DOWN_PATTERN => self.handle_keystroke("Down", config.down_key, enigo).await,
            _ => {}
        }
    }

    async fn setup_peripheral(
        &self,
        peripheral: &PlatformPeripheral,
    ) -> Result<Option<btleplug::api::Characteristic>, Box<dyn Error + Send + Sync>> {
        peripheral.connect().await?;
        peripheral.discover_services().await?;

        let chars = peripheral.characteristics();
        let notify_uuid = get_notify_uuid();
        let battery_uuid = get_battery_uuid();

        // 1. Setup Keystroke Notifications
        let notify_char = self
            .find_characteristic(&chars, notify_uuid)
            .ok_or("Could not find notification characteristic")?;
        peripheral.subscribe(notify_char).await?;

        // 2. Setup Battery Notifications
        let battery_char = self.find_characteristic(&chars, battery_uuid).cloned();
        if let Some(bc) = &battery_char
            && peripheral.subscribe(bc).await.is_ok()
        {
            self.read_battery_level(peripheral, &battery_char).await;
            return Ok(Some(bc.clone()));
        }

        Ok(None)
    }

    async fn process_notifications(
        &self,
        peripheral: &PlatformPeripheral,
        battery_char: &Option<btleplug::api::Characteristic>,
        enigo: &mut Enigo,
    ) -> Result<(), Box<dyn Error + Send + Sync>> {
        let mut notifications = peripheral.notifications().await?;
        let mut battery_poll_interval = tokio::time::interval(std::time::Duration::from_secs(300));

        loop {
            tokio::select! {
                maybe_notification = notifications.next() => {
                    match maybe_notification {
                        Some(n) => self.handle_notification(n.uuid, &n.value, enigo).await,
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

    async fn handle_peripheral(
        self: Arc<Self>,
        peripheral: PlatformPeripheral,
    ) -> Result<(), Box<dyn Error + Send + Sync>> {
        let mut enigo = Enigo::new(&Settings::default())
            .map_err(|e| format!("Failed to initialize keyboard emulator (Enigo): {}", e))?;

        {
            let config = self.rx_config.borrow();
            let _ = enigo.key(config.up_key, Direction::Release);
            let _ = enigo.key(config.down_key, Direction::Release);
        }

        let battery_char = self.setup_peripheral(&peripheral).await?;
        self.process_notifications(&peripheral, &battery_char, &mut enigo)
            .await
    }
}

/// Internal event used by the worker to communicate between tasks
enum InternalEvent {
    DeviceFound(PlatformPeripheral),
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
    pub async fn new(
        rx_config: watch::Receiver<BleConfig>,
        tx_event: broadcast::Sender<BleEvent>,
        rx_cmd: mpsc::UnboundedReceiver<BleCommand>,
    ) -> Result<Self, Box<dyn Error + Send + Sync>> {
        let manager = PlatformManager::new().await?;
        let adapters = manager.adapters().await?;
        let adapter = adapters
            .into_iter()
            .next()
            .ok_or("No Bluetooth adapter found")?;

        let (internal_tx, internal_rx) = mpsc::unbounded_channel::<InternalEvent>();
        let ctx = Arc::new(BleContext::new(tx_event, rx_config));

        Ok(Self {
            ctx,
            rx_cmd,
            adapter,
            internal_tx,
            internal_rx,
        })
    }

    async fn find_target_device(
        adapter: &btleplug::platform::Adapter,
    ) -> Option<PlatformPeripheral> {
        let peripherals = adapter.peripherals().await.ok()?;
        for p in peripherals {
            if let Some(props) = p.properties().await.ok().flatten()
                && let Some(name) = props.local_name
                && name.contains(DEVICE_NAME_FILTER)
            {
                return Some(p);
            }
        }
        None
    }

    async fn execute_scan(
        adapter: btleplug::platform::Adapter,
        internal_tx: mpsc::UnboundedSender<InternalEvent>,
    ) {
        if let Err(e) = adapter.start_scan(ScanFilter::default()).await {
            let _ = internal_tx.send(InternalEvent::Log(
                format!("Scan start error: {}", e).into(),
            ));
            return;
        }

        let timeout = std::time::Duration::from_secs(20);
        let start_time = std::time::Instant::now();

        while start_time.elapsed() < timeout {
            if let Some(device) = Self::find_target_device(&adapter).await {
                let _ = internal_tx.send(InternalEvent::DeviceFound(device));
                return;
            }
            tokio::time::sleep(std::time::Duration::from_millis(500)).await;
        }
        let _ = internal_tx.send(InternalEvent::StatusUpdate(State::Disconnected));
    }

    async fn start_connection_session(&self, peripheral: PlatformPeripheral) {
        let addr: Cow<'static, str> = peripheral.address().to_string().into();
        let ctx = self.ctx.clone();
        let tx_event = ctx.tx_event.clone();

        tokio::spawn(async move {
            let mut retry_count = 0;
            let max_retries = 5;
            let mut backoff = std::time::Duration::from_secs(1);

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

    pub async fn run(mut self) -> Result<(), Box<dyn Error + Send + Sync>> {
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
