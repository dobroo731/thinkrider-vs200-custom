use btleplug::api::{Central, Manager, Peripheral, ScanFilter};
use btleplug::platform::{Manager as PlatformManager, Peripheral as PlatformPeripheral};
use enigo::{Enigo, Keyboard, Key, Settings, Direction};
use tokio::sync::{mpsc, watch};
use std::error::Error;
use futures::stream::StreamExt;

pub const DEVICE_NAME_FILTER: &str = "Think VS01-";
pub const NOTIFY_UUID_STR: &str = "0000fea1-0000-1000-8000-00805f9b34fb";
pub const BATTERY_UUID_STR: &str = "00002a19-0000-1000-8000-00805f9b34fb";
pub const SHIFT_UP_PATTERN: &[u8] = &[0xf3, 0x05, 0x03, 0x01, 0xfc];
pub const SHIFT_DOWN_PATTERN: &[u8] = &[0xf3, 0x05, 0x03, 0x00, 0xfb];

#[derive(Debug, Clone)]
pub enum State {
    Disconnected,
    Searching,
    Connecting { device_address: String },
    Connected { device_address: String },
    Reconnecting { device_address: String },
}

#[derive(Debug, Clone)]
pub enum BleEvent {
    StatusUpdate(State),
    Log(String),
    BatteryLevel(u8),
    KeyPress(String),
}

#[derive(Debug, Clone)]
pub enum BleCommand {
    SearchDevice,
    StopSearch,
}

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

/// Internal event used by the worker to communicate between tasks
enum InternalEvent {
    DeviceFound(PlatformPeripheral),
    StatusUpdate(State),
    Log(String),
}

pub async fn run_ble_worker(
    config: BleConfig,
    tx_event: mpsc::UnboundedSender<BleEvent>,
    mut rx_cmd: mpsc::UnboundedReceiver<BleCommand>,
    rx_notifications_enabled: watch::Receiver<bool>,
) -> Result<(), Box<dyn Error + Send + Sync>> {
    let manager = PlatformManager::new().await?;
    let adapters = manager.adapters().await?;
    let adapter = adapters.into_iter().next().ok_or("No Bluetooth adapter found")?;

    let (internal_tx, mut internal_rx) = mpsc::unbounded_channel::<InternalEvent>();
    let mut scan_task: Option<tokio::task::JoinHandle<()>> = None;
    let mut connection_task: Option<tokio::task::JoinHandle<()>> = None;

    loop {
        tokio::select! {
            Some(cmd) = rx_cmd.recv() => {
                match cmd {
                    BleCommand::SearchDevice => {
                        if let Some(task) = scan_task.take() {
                            task.abort();
                        }
                        if let Some(task) = connection_task.take() {
                            task.abort();
                        }

                        if *rx_notifications_enabled.borrow() {
                            let _ = tx_event.send(BleEvent::Log("User triggered 'Search Device'".to_string()));
                        }

                        let _ = tx_event.send(BleEvent::StatusUpdate(State::Searching));

                        let adapter_clone = adapter.clone();
                        let internal_tx_clone = internal_tx.clone();

                        scan_task = Some(tokio::spawn(async move {
                            if let Err(e) = adapter_clone.start_scan(ScanFilter::default()).await {
                                let _ = internal_tx_clone.send(InternalEvent::Log(format!("Scan start error: {}", e)));
                                return;
                            }

                            let timeout = std::time::Duration::from_secs(20);
                            let start_time = std::time::Instant::now();

                            while start_time.elapsed() < timeout {
                                if let Ok(peripherals) = adapter_clone.peripherals().await {
                                    for p in peripherals {
                                        if let Some(props) = p.properties().await.ok().flatten()
                                            && let Some(name) = props.local_name
                                            && name.contains(DEVICE_NAME_FILTER)
                                        {
                                            let _ = internal_tx_clone.send(InternalEvent::DeviceFound(p));
                                            return;
                                        }
                                    }
                                }
                                tokio::time::sleep(std::time::Duration::from_millis(500)).await;
                            }
                            let _ = internal_tx_clone.send(InternalEvent::StatusUpdate(State::Disconnected));
                        }));
                    }
                    BleCommand::StopSearch => {
                        if let Some(task) = scan_task.take() {
                            task.abort();
                        }
                        if let Some(task) = connection_task.take() {
                            task.abort();
                        }
                        let _ = tx_event.send(BleEvent::StatusUpdate(State::Disconnected));
                        let _ = tx_event.send(BleEvent::Log("Search stopped by user".to_string()));
                    }
                }
            }
            Some(internal_event) = internal_rx.recv() => {
                match internal_event {
                    InternalEvent::DeviceFound(peripheral) => {
                        let addr = peripheral.address().to_string();
                        let _ = tx_event.send(BleEvent::StatusUpdate(State::Connected { device_address: addr }));

                        let tx_event_clone = tx_event.clone();
                        let config_clone = BleConfig {
                            up_key: config.up_key,
                            down_key: config.down_key,
                            debug: config.debug,
                            notifications_enabled: config.notifications_enabled
                        };

                        connection_task = Some(tokio::spawn(async move {
                            let mut retry_count = 0;
                            let max_retries = 5;
                            let mut backoff = std::time::Duration::from_secs(1);

                            loop {
                                if retry_count > 0 {
                                    let _ = tx_event_clone.send(BleEvent::StatusUpdate(State::Reconnecting {
                                        device_address: peripheral.address().to_string()
                                    }));
                                } else {
                                    let _ = tx_event_clone.send(BleEvent::StatusUpdate(State::Connected {
                                        device_address: peripheral.address().to_string()
                                    }));
                                }

                                match handle_peripheral(peripheral.clone(), tx_event_clone.clone(), &config_clone).await {
                                    Ok(_) => {
                                        let _ = tx_event_clone.send(BleEvent::Log("Peripheral session ended gracefully".to_string()));
                                        break;
                                    }
                                    Err(e) => {
                                        let _ = tx_event_clone.send(BleEvent::Log(format!("Connection lost: {}. Retrying... ({} / {})", e, retry_count + 1, max_retries)));
                                        retry_count += 1;
                                        if retry_count > max_retries {
                                            let _ = tx_event_clone.send(BleEvent::Log("Max reconnection attempts reached. Giving up.".to_string()));
                                            break;
                                        }
                                        tokio::time::sleep(backoff).await;
                                        backoff *= 2;
                                    }
                                }
                            }
                            let _ = tx_event_clone.send(BleEvent::StatusUpdate(State::Disconnected));
                        }));
                    }
                    InternalEvent::StatusUpdate(s) => {
                        let _ = tx_event.send(BleEvent::StatusUpdate(s));
                    }
                    InternalEvent::Log(msg) => {
                        let _ = tx_event.send(BleEvent::Log(msg));
                    }
                }
            }
        }
    }
}

async fn handle_peripheral(
    peripheral: PlatformPeripheral,
    tx_event: mpsc::UnboundedSender<BleEvent>,
    config: &BleConfig,
) -> Result<(), Box<dyn Error + Send + Sync>> {
    let mut enigo = Enigo::new(&Settings::default())
        .map_err(|e| format!("Failed to initialize keyboard emulator (Enigo): {}", e))?;

    // Safety release: ensure no keys are stuck from previous sessions
    let _ = enigo.key(config.up_key, Direction::Release);
    let _ = enigo.key(config.down_key, Direction::Release);

    peripheral.connect().await?;
    peripheral.discover_services().await?;

    let chars = peripheral.characteristics();

    // 1. Setup Keystroke Notifications
    let notify_char = chars.iter().find(|c| c.uuid.to_string() == NOTIFY_UUID_STR)
        .ok_or("Could not find notification characteristic")?;
    peripheral.subscribe(notify_char).await?;

    // 2. Setup Battery Notifications
    let battery_char = chars.iter().find(|c| c.uuid.to_string() == BATTERY_UUID_STR).cloned();
    if let Some(bc) = &battery_char
        && peripheral.subscribe(bc).await.is_ok()
        && let Ok(val) = peripheral.read(bc).await
        && let Some(&level) = val.first() {
        let _ = tx_event.send(BleEvent::BatteryLevel(level));
    }

    let mut notifications = peripheral.notifications().await?;
    let mut battery_poll_interval = tokio::time::interval(std::time::Duration::from_secs(300));

    loop {
        tokio::select! {
            maybe_notification = notifications.next() => {
                match maybe_notification {
                    Some(notification) => {
                        if notification.uuid.to_string() == BATTERY_UUID_STR {
                            if let Some(&level) = notification.value.first() {
                                let _ = tx_event.send(BleEvent::BatteryLevel(level));
                            }
                            continue;
                        }

                        if notification.value == SHIFT_UP_PATTERN {
                            if config.debug { let _ = tx_event.send(BleEvent::Log("Pressed: Up".to_string())); }
                            let _ = tx_event.send(BleEvent::KeyPress("Up".to_string()));
                            let _ = enigo.key(config.up_key, Direction::Press);
                            let _ = enigo.key(config.up_key, Direction::Release);
                        } else if notification.value == SHIFT_DOWN_PATTERN {
                            if config.debug { let _ = tx_event.send(BleEvent::Log("Pressed: Down".to_string())); }
                            let _ = tx_event.send(BleEvent::KeyPress("Down".to_string()));
                            let _ = enigo.key(config.down_key, Direction::Press);
                            let _ = enigo.key(config.down_key, Direction::Release);
                        }
                    }
                    None => {
                        let _ = tx_event.send(BleEvent::Log("BLE notification stream closed".to_string()));
                        return Err("Notification stream terminated".into());
                    }
                }
            }
            _ = battery_poll_interval.tick() => {
                if let Some(bc) = &battery_char
                    && let Ok(val) = peripheral.read(bc).await
                    && let Some(&level) = val.first() {
                    let _ = tx_event.send(BleEvent::BatteryLevel(level));
                }
            }
        }
    }
}
