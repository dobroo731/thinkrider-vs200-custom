use async_trait::async_trait;
use btleplug::api::{Central, Characteristic, Manager, Peripheral, ScanFilter, ValueNotification};
use btleplug::platform::{Manager as PlatformManager, Peripheral as PlatformPeripheral};
use enigo::{Direction, Enigo, Key, Keyboard, Settings};
use futures::stream::StreamExt;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::{broadcast, mpsc};
use tokio_util::future::FutureExt;
use tokio_util::sync::CancellationToken;
use tokio_util::task::TaskTracker;
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
const MAX_RECONNECTION_ATTEMPTS: u32 = 5;
const INITIAL_BACKOFF: Duration = Duration::from_secs(1);
const BATTERY_POLL_INTERVAL: Duration = Duration::from_secs(300);
const EVENT_CHANNEL_CAPACITY: usize = 16;

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
        characteristics: &std::collections::BTreeSet<Characteristic>,
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
    address: String,
    battery: Option<Characteristic>,
}

impl BleDevice {
    pub fn new(profile: Arc<dyn BleDeviceProfile>, peripheral: PlatformPeripheral) -> Self {
        let address = peripheral.address().to_string();
        Self {
            address,
            profile,
            peripheral,
            battery: None,
        }
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
        tracing::debug!(address = %self.address, "Connecting to peripheral");
        self.peripheral.connect().await?;
        tracing::debug!(address = %self.address, "Connected successfully");

        tracing::debug!(address = %self.address, "Discovering services");
        self.peripheral.discover_services().await?;
        tracing::debug!(address = %self.address, "Services discovered");

        let device_chars = self
            .profile
            .find_characteristics(&self.peripheral.characteristics())
            .ok_or(BleError::ConnectionFailed(
                "Could not find required characteristics for this device".into(),
            ))?;

        tracing::debug!(address = %self.address, "Subscribing to notification characteristic");
        self.peripheral.subscribe(&device_chars.notify).await?;
        tracing::debug!(address = %self.address, "Notification subscription successful");

        // Optional battery subscription/storage
        if let Some(battery_characteristic) = device_chars.battery
            && self
                .peripheral
                .subscribe(&battery_characteristic)
                .await
                .is_ok()
        {
            tracing::debug!(address = %self.address, "Battery subscription successful");
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
    pub fn new() -> BleResult<Self> {
        let enigo = Enigo::new(&Settings::default()).map_err(|error| {
            BleError::InteractionError(format!(
                "Failed to initialize keyboard emulator (Enigo): {}",
                error
            ))
        })?;
        Ok(Self {
            enigo,
            up_key: Key::K,
            down_key: Key::I,
        })
    }

    async fn simulate_keypress(&mut self, key: Key) -> BleResult<()> {
        self.enigo
            .key(key, Direction::Press)
            .map_err(|error| BleError::InteractionError(error.to_string()))?;
        self.enigo
            .key(key, Direction::Release)
            .map_err(|error| BleError::InteractionError(error.to_string()))?;
        Ok(())
    }
}

#[async_trait]
impl InteractionHandler for KeyboardInteraction {
    async fn shift_up(&mut self) -> BleResult<()> {
        self.simulate_keypress(self.up_key).await
    }

    async fn shift_down(&mut self) -> BleResult<()> {
        self.simulate_keypress(self.down_key).await
    }
}

pub struct InteractionManager<T: InteractionHandler> {
    event_receiver: broadcast::Receiver<BleEvent>,
    handler: T,
}

impl<T: InteractionHandler> InteractionManager<T> {
    fn new(event_receiver: broadcast::Receiver<BleEvent>, handler: T) -> Self {
        Self {
            event_receiver,
            handler,
        }
    }

    async fn run(mut self, shutdown_token: CancellationToken) {
        while let Some(recv_result) = self
            .event_receiver
            .recv()
            .with_cancellation_token(&shutdown_token)
            .await
        {
            match recv_result {
                Ok(BleEvent::KeyPress(direction)) => {
                    let result = match direction {
                        ShiftDirection::Up => self.handler.shift_up().await,
                        ShiftDirection::Down => self.handler.shift_down().await,
                    };
                    if let Err(e) = result {
                        tracing::error!(%direction, %e, "Interaction Error");
                    }
                }
                Ok(_) => {} // Ignore events not relevant to interaction (e.g., BatteryLevel)
                Err(broadcast::error::RecvError::Lagged(_)) => {
                    tracing::warn!("Interaction Manager lagged: some events were missed");
                }
                Err(broadcast::error::RecvError::Closed) => {
                    tracing::info!("Event channel closed; shutting down Interaction Manager");
                    break;
                }
            }
        }

        if shutdown_token.is_cancelled() {
            tracing::info!("Interaction Manager shutting down via token");
        }
    }
}

#[derive(Debug, Clone, strum::Display)]
pub enum State {
    Disconnected,
    Searching,
    Connecting { device_address: String },
    Connected { device_address: String },
}

#[derive(Debug, Clone)]
pub enum BleEvent {
    StatusUpdate(State),
    BatteryLevel(u8),
    KeyPress(ShiftDirection),
}

#[derive(Debug, Clone)]
pub enum BleCommand {
    SearchDevice,
    StopSearch,
}

/// A handle for interacting with the BLE system from the UI/CLI.
/// This is fully cloneable and can be passed around freely.
#[derive(Clone)]
pub struct BleSystemHandle {
    pub event_sender: broadcast::Sender<BleEvent>,
    pub command_sender: mpsc::UnboundedSender<BleCommand>,
}

/// A helper to encapsulate the standard setup of BLE components.
pub struct BleSystem {
    handle: BleSystemHandle,
    command_receiver: Option<mpsc::UnboundedReceiver<BleCommand>>,
    shutdown_token: CancellationToken,
}

impl BleSystem {
    pub fn handle(&self) -> BleSystemHandle {
        self.handle.clone()
    }

    /// Explicitly shut down the BLE system and all its background tasks.
    pub fn shutdown(&self) {
        self.shutdown_token.cancel();
    }

    /// Boots the BLE worker and interaction manager as background tasks.
    pub async fn bootstrap<P: BleDeviceProfile + 'static>(
        mut self,
        profile: P,
        handler: impl InteractionHandler + 'static,
    ) -> BleResult<ActiveTask> {
        let handle = self.handle.clone();
        let task = ActiveTask::from(self.shutdown_token.clone());

        // Take the receiver out of Option to move it into the worker task
        let command_receiver = self
            .command_receiver
            .take()
            .ok_or_else(|| BleError::InteractionError("BLE System already bootstrapped".into()))?;

        let worker = BleWorker::new(profile, handle.event_sender.clone(), command_receiver).await?;

        let worker_token = task.cancelation_token.clone();
        task.tracker.spawn(async move {
            if let Err(error) = worker.run(worker_token).await {
                tracing::error!(%error, "BleWorker critical error");
            }
        });

        // 2. Spawn Interaction Manager
        let interaction_manager = InteractionManager::new(handle.event_sender.subscribe(), handler);

        let manager_token = task.cancelation_token.clone();
        task.tracker.spawn(async move {
            interaction_manager.run(manager_token).await;
        });

        Ok(task)
    }
}

impl Default for BleSystem {
    fn default() -> Self {
        let (event_sender, _) = broadcast::channel(EVENT_CHANNEL_CAPACITY); // Receiver is created via subscribe()
        let (command_sender, command_receiver) = mpsc::unbounded_channel();

        Self {
            handle: BleSystemHandle {
                event_sender,
                command_sender,
            },
            command_receiver: Some(command_receiver),
            shutdown_token: CancellationToken::new(),
        }
    }
}

impl Drop for BleSystem {
    fn drop(&mut self) {
        tracing::info!("BleSystem dropped: triggering automatic shutdown");
        self.shutdown();
    }
}

pub struct BleContext {
    profile: Arc<dyn BleDeviceProfile>,
    event_sender: broadcast::Sender<BleEvent>,
}

impl BleContext {
    fn new(profile: Arc<dyn BleDeviceProfile>, event_sender: broadcast::Sender<BleEvent>) -> Self {
        Self {
            profile,
            event_sender,
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
                tracing::error!(%error, "Battery read error");
                Err(error)
            }
        }
    }

    fn handle_notification(&self, device: &BleDevice, uuid: Uuid, value: &[u8]) {
        if let Some(event) = device.parse_notification(uuid, value) {
            if let BleEvent::KeyPress(direction) = event {
                tracing::debug!(%direction, "Pressed");
            }
            let _ = self.event_sender.send(event);
        }
    }

    async fn setup_device(&self, device: &mut BleDevice) -> BleResult<()> {
        device.connect_and_subscribe().await?;
        let _ = self.read_battery_level(device).await;
        Ok(())
    }

    async fn process_notifications(
        &self,
        device: &mut BleDevice,
        token: CancellationToken,
    ) -> BleResult<()> {
        let mut stream = device.peripheral.notifications().await?;

        while let Some(notification) = stream
            .next()
            .with_cancellation_token(&token)
            .await
            .flatten()
        {
            self.handle_notification(device, notification.uuid, &notification.value);
        }

        if token.is_cancelled() {
            tracing::trace!(address = %device.address(), "Notification listener cancelled");
        } else {
            tracing::warn!(address = %device.address(), "Notification stream ended unexpectedly");
        }

        Ok(())
    }

    async fn handle_peripheral(
        self: Arc<Self>,
        mut device: BleDevice,
        task: ActiveTask,
    ) -> BleResult<()> {
        self.setup_device(&mut device).await?;

        // Signal that the connection is successfully established and session has started
        let _ = self
            .event_sender
            .send(BleEvent::StatusUpdate(State::Connected {
                device_address: device.address().to_string(),
            }));

        let device_for_poll = device.clone();
        let context_for_poll = self.clone();
        let token_for_poll = task.cancelation_token.clone();

        task.tracker.spawn(async move {
            let mut poll_interval = tokio::time::interval(BATTERY_POLL_INTERVAL);
            while poll_interval
                .tick()
                .with_cancellation_token(&token_for_poll)
                .await
                .is_some()
            {
                if context_for_poll
                    .read_battery_level(&device_for_poll)
                    .await
                    .is_err()
                {
                    break;
                }
            }

            if token_for_poll.is_cancelled() {
                tracing::trace!(address = %device_for_poll.address(), "Battery polling cancelled");
            }
        });

        self.process_notifications(&mut device, task.cancelation_token.clone())
            .await
    }
}

enum InternalEvent {
    DeviceFound(BleDevice),
    StatusUpdate(State),
}

#[derive(Clone)]
pub struct ActiveTask {
    tracker: TaskTracker,
    cancelation_token: CancellationToken,
}

impl Drop for ActiveTask {
    fn drop(&mut self) {
        self.cancelation_token.cancel();
        self.tracker.close();
    }
}

impl From<CancellationToken> for ActiveTask {
    fn from(value: CancellationToken) -> Self {
        ActiveTask {
            tracker: TaskTracker::new(),
            cancelation_token: value,
        }
    }
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
        let context = Arc::new(BleContext::new(Arc::new(profile), event_sender));
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
        token: CancellationToken,
    ) {
        tracing::debug!("Starting BLE scan");
        if let Err(error) = adapter.start_scan(ScanFilter::default()).await {
            tracing::error!(%error, "Scan error");
            return;
        }

        let mut events = match adapter.events().await {
            Ok(e) => e,
            Err(error) => {
                tracing::error!(%error, "Failed to get adapter events");
                return;
            }
        };

        let scan_future = async {
            while let Some(event) = events
                .next()
                .with_cancellation_token(&token)
                .await
                .flatten()
            {
                if let btleplug::api::CentralEvent::DeviceDiscovered(peripheral_id) = event {
                    let Ok(peripheral) = adapter.peripheral(&peripheral_id).await else {
                        continue;
                    };
                    let Ok(Some(properties)) = peripheral.properties().await else {
                        continue;
                    };

                    if let Some(name) = properties.local_name {
                        tracing::debug!(device_name = %name, "Discovered BLE device");
                        if profile.matches(Some(&name)) {
                            tracing::info!(device_name = %name, "Found target device matching profile!");
                            return Ok(BleDevice::new(profile.clone(), peripheral));
                        }
                    }
                }
            }

            if token.is_cancelled() {
                tracing::trace!("Scan cancelled via token");
            }
            Err(BleError::DeviceNotFound)
        };

        match tokio::time::timeout(SCAN_TIMEOUT, scan_future).await {
            Ok(Ok(found_device)) => {
                tracing::debug!(address = %found_device.address(), "Scan successful: sending device to worker");
                let _ = worker_event_sender.send(InternalEvent::DeviceFound(found_device));
            }
            Ok(Err(e)) => {
                tracing::debug!(%e, "Scan finished without finding device");
                let _ = worker_event_sender.send(InternalEvent::StatusUpdate(State::Disconnected));
            }
            Err(_) => {
                tracing::debug!(timeout = ?SCAN_TIMEOUT, "Scan timed out");
                let _ = worker_event_sender.send(InternalEvent::StatusUpdate(State::Disconnected));
            }
        }
    }

    async fn start_connection_session(
        context: Arc<BleContext>,
        device: BleDevice,
        task: ActiveTask,
    ) -> impl Future<Output = ()> {
        let address = device.address().to_string();
        let event_sender = context.event_sender.clone();

        async move {
            let mut retry_count = 0;
            let mut backoff = INITIAL_BACKOFF;

            loop {
                let _ = event_sender.send(BleEvent::StatusUpdate(State::Connecting {
                    device_address: address.clone(),
                }));

                let attempt = context
                    .clone()
                    .handle_peripheral(device.clone(), task.clone())
                    .with_cancellation_token(&task.cancelation_token)
                    .await;

                match attempt {
                    None | Some(Ok(_)) => break, // Normal closure of the session
                    Some(Err(e)) if retry_count < MAX_RECONNECTION_ATTEMPTS => {
                        tracing::warn!(%e, "Connection lost. Retrying...");
                        retry_count += 1;

                        if tokio::time::sleep(backoff)
                            .with_cancellation_token(&task.cancelation_token)
                            .await
                            .is_none()
                        {
                            return; // Cancelled during backoff sleep
                        }
                        backoff *= 2;
                    }
                    Some(Err(e)) => {
                        tracing::error!(%e, "Max reconnection attempts reached");
                        break;
                    }
                }
            }

            if !task.cancelation_token.is_cancelled() {
                let _ = event_sender.send(BleEvent::StatusUpdate(State::Disconnected));
            }
        }
    }

    pub async fn run(mut self, shutdown_token: CancellationToken) -> BleResult<()> {
        let mut active_task = None;

        loop {
            tokio::select! {
                _ = shutdown_token.cancelled() => {
                    tracing::info!("BleWorker shutting down via global token");
                    break;
                }
                Some(command) = self.command_receiver.recv() => {
                    match command {
                        BleCommand::SearchDevice => {
                            let _ = self.context.event_sender.send(BleEvent::StatusUpdate(State::Searching));
                            let token = shutdown_token.child_token();
                            let task = ActiveTask::from(token);
                            task.tracker.spawn(Self::execute_scan(
                                self.adapter.clone(),
                                self.context.profile.clone(),
                                self.worker_event_sender.clone(),
                                task.cancelation_token.clone(),
                            ));
                            active_task = Some(task);
                        }
                        BleCommand::StopSearch => {
                            let _ = self.context.event_sender.send(BleEvent::StatusUpdate(State::Disconnected));
                            active_task.take(); // RAII handles the abort/cancel via ActiveTask::drop
                        }
                    }
                }
                Some(internal_event) = self.worker_event_receiver.recv() => match internal_event {
                    InternalEvent::DeviceFound(device) => {
                        let token = shutdown_token.child_token();
                        let task = ActiveTask::from(token);
                        task.tracker.spawn(
                            Self::start_connection_session(self.context.clone(), device, task.clone())
                        );
                        active_task = Some(task);
                    }
                    InternalEvent::StatusUpdate(status) => {
                        let _ = self.context.event_sender.send(BleEvent::StatusUpdate(status));
                    }
                }
            }
        }

        active_task.take();
        Ok(())
    }
}
