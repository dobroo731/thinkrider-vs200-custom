use btleplug::api::{Central, Manager as _, Peripheral, ScanFilter};
use btleplug::platform::{Manager};
use enigo::{Enigo, Keyboard, Key, Settings, Direction};
use futures::stream::StreamExt;
use tokio::sync::{mpsc, watch};
use tray_icon::{TrayIconBuilder, TrayIconEvent, Icon};
use tray_icon::menu::{MenuEvent, MenuId};
use winit::{
    event::Event as WinitEvent,
    event_loop::{ControlFlow, EventLoop},
};

// Constants from cli.rs baseline
const DEVICE_NAME_FILTER: &str = "Think VS01-";
const NOTIFY_UUID_STR: &str = "0000fea1-0000-1000-8000-00805f9b34fb";
const BATTERY_UUID_STR: &str = "00002a19-0000-1000-8000-00805f9b34fb";
const SHIFT_UP_PATTERN: &[u8] = &[0xf3, 0x05, 0x03, 0x01, 0xfc];
const SHIFT_DOWN_PATTERN: &[u8] = &[0xf3, 0x05, 0x03, 0x00, 0xfb];

#[derive(Debug, Clone)]
enum State {
    Disconnected,
    Searching,
    Connected { device_address: String },
}

struct AppState {
    notifications_enabled: bool,
    // Removed connection_state as it was assigned but never read (Clippy unused_assignments)
}

/// Commands sent from the UI to the BLE Worker
enum Command {
    SearchDevice,
    StopSearch,
}

/// Events sent from the BLE Worker back to the UI (and internally)
enum WorkerEvent {
    StatusUpdate(State),
    Log(String),
    InternalDeviceFound(btleplug::platform::Peripheral),
    BatteryLevel(u8),
}

async fn ble_worker(
    tx_event: mpsc::UnboundedSender<WorkerEvent>,
    mut rx_cmd: mpsc::UnboundedReceiver<Command>,
    rx_notifications: watch::Receiver<bool>,
) {
    let manager = Manager::new().await.expect("Failed to create BLE manager");
    let adapters = manager.adapters().await.expect("Failed to get adapters");
    let adapter = adapters.into_iter().next().expect("No Bluetooth adapter found");

    let (internal_tx, mut internal_rx) = mpsc::unbounded_channel::<WorkerEvent>();
    let mut scan_task: Option<tokio::task::JoinHandle<()>> = None;

    loop {
        tokio::select! {
            // Handle commands from UI
            Some(cmd) = rx_cmd.recv() => {
                match cmd {
                    Command::SearchDevice => {
                        if let Some(task) = scan_task.take() {
                            task.abort();
                        }

                        if *rx_notifications.borrow() {
                            let _ = tx_event.send(WorkerEvent::Log("User triggered 'Search Device'".to_string()));
                        }

                        let _ = tx_event.send(WorkerEvent::StatusUpdate(State::Searching));

                        let adapter_clone = adapter.clone();
                        let internal_tx_clone = internal_tx.clone();

                        scan_task = Some(tokio::spawn(async move {
                            if let Err(e) = adapter_clone.start_scan(ScanFilter::default()).await {
                                let _ = internal_tx_clone.send(WorkerEvent::Log(format!("Scan start error: {}", e)));
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
                                            let _ = internal_tx_clone.send(WorkerEvent::InternalDeviceFound(p));
                                            return;
                                        }
                                    }
                                }
                                tokio::time::sleep(std::time::Duration::from_millis(500)).await;
                            }
                            let _ = internal_tx_clone.send(WorkerEvent::StatusUpdate(State::Disconnected));
                        }));
                    }
                    Command::StopSearch => {
                        if let Some(task) = scan_task.take() {
                            task.abort();
                        }
                        let _ = tx_event.send(WorkerEvent::StatusUpdate(State::Disconnected));
                        let _ = tx_event.send(WorkerEvent::Log("Search stopped by user".to_string()));
                    }
                }
            }
            // Handle internal events from the scan task
            Some(internal_event) = internal_rx.recv() => {
                match internal_event {
                    WorkerEvent::InternalDeviceFound(peripheral) => {
                        let addr = peripheral.address().to_string();
                        let _ = tx_event.send(WorkerEvent::StatusUpdate(State::Connected { device_address: addr }));

                        let rx_notifications_clone = rx_notifications.clone();
                        let tx_event_clone = tx_event.clone();
                        tokio::spawn(async move {
                            if let Err(e) = handle_peripheral(peripheral, tx_event_clone.clone(), rx_notifications_clone).await {
                                let _ = tx_event_clone.send(WorkerEvent::Log(format!("Peripheral error: {}", e)));
                            }
                            let _ = tx_event_clone.send(WorkerEvent::StatusUpdate(State::Disconnected));
                        });
                    }
                    WorkerEvent::StatusUpdate(s) => {
                        let _ = tx_event.send(WorkerEvent::StatusUpdate(s));
                    }
                    WorkerEvent::Log(msg) => {
                        let _ = tx_event.send(WorkerEvent::Log(msg));
                    }
                    WorkerEvent::BatteryLevel(level) => {
                        let _ = tx_event.send(WorkerEvent::BatteryLevel(level));
                    }
                }
            }
        }
    }
}

async fn handle_peripheral(
    peripheral: btleplug::platform::Peripheral,
    tx_event: mpsc::UnboundedSender<WorkerEvent>,
    rx_notifications: watch::Receiver<bool>,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let mut enigo = Enigo::new(&Settings::default()).unwrap();

    peripheral.connect().await?;
    peripheral.discover_services().await?;

    let chars = peripheral.characteristics();

    // 1. Setup Keystroke Notifications
    let notify_char = chars.iter().find(|c| c.uuid.to_string() == NOTIFY_UUID_STR)
        .ok_or("Could not find notification characteristic")?;
    peripheral.subscribe(notify_char).await?;

    // 2. Setup Battery Notifications (if supported)
    let battery_char = chars.iter().find(|c| c.uuid.to_string() == BATTERY_UUID_STR).cloned();
    if let Some(bc) = &battery_char {
        if peripheral.subscribe(bc).await.is_ok() {
            if let Ok(val) = peripheral.read(bc).await {
                if let Some(&level) = val.first() {
                    let _ = tx_event.send(WorkerEvent::BatteryLevel(level));
                }
            }
        }
    }

    let mut notifications = peripheral.notifications().await?;

    // Create a polling interval for battery level (every 5 minutes) to ensure updates
    // even if the device doesn't push notifications.
    let mut battery_poll_interval = tokio::time::interval(std::time::Duration::from_secs(300));

    loop {
        tokio::select! {
            Some(notification) = notifications.next() => {
                if notification.uuid.to_string() == BATTERY_UUID_STR {
                    if let Some(&level) = notification.value.first() {
                        let _ = tx_event.send(WorkerEvent::BatteryLevel(level));
                    }
                    continue;
                }

                if !*rx_notifications.borrow() { continue; }

                if notification.value == SHIFT_UP_PATTERN {
                    let _ = enigo.key(Key::K, Direction::Press);
                    let _ = enigo.key(Key::K, Direction::Release);
                } else if notification.value == SHIFT_DOWN_PATTERN {
                    let _ = enigo.key(Key::I, Direction::Press);
                    let _ = enigo.key(Key::I, Direction::Release);
                }
            }
            _ = battery_poll_interval.tick() => {
                if let Some(bc) = &battery_char {
                    if let Ok(val) = peripheral.read(bc).await {
                        if let Some(&level) = val.first() {
                            let _ = tx_event.send(WorkerEvent::BatteryLevel(level));
                        }
                    }
                }
            }
        }
    }
}

fn main() {
    let event_loop = EventLoop::new().unwrap();

    let mut app_state = AppState {
        notifications_enabled: true,
    };

    let (cmd_tx, cmd_rx) = mpsc::unbounded_channel();
    let (event_tx, mut event_rx) = mpsc::unbounded_channel();
    let (notify_tx, notify_rx) = watch::channel(app_state.notifications_enabled);

    std::thread::spawn(move || {
        let rt = tokio::runtime::Runtime::new().unwrap();
        rt.block_on(ble_worker(event_tx, cmd_rx, notify_rx));
    });

    let icon_data = vec![255u8; 32 * 32 * 4];
    let icon = Icon::from_rgba(icon_data, 32, 32).expect("Failed to create icon");

    let menu = tray_icon::menu::Menu::new();
    const ID_NOTIFY: u32 = 0;
    const ID_SEARCH: u32 = 1;
    const ID_STATUS: u32 = 2;
    const ID_QUIT: u32 = 3;
    const ID_STOP_SEARCH: u32 = 4;
    const ID_BATTERY: u32 = 5;

    let notify_item = tray_icon::menu::MenuItem::with_id(ID_NOTIFY, "Notification [ON]", true, None);
    menu.append(&notify_item).unwrap();

    let search_item = tray_icon::menu::MenuItem::with_id(ID_SEARCH, "Search Device", true, None);
    menu.append(&search_item).unwrap();

    let stop_search_item = tray_icon::menu::MenuItem::with_id(ID_STOP_SEARCH, "Stop Searching", false, None);
    menu.append(&stop_search_item).unwrap();

    let status_item = tray_icon::menu::MenuItem::with_id(ID_STATUS, "Status: Disconnected", false, None);
    menu.append(&status_item).unwrap();

    let battery_item = tray_icon::menu::MenuItem::with_id(ID_BATTERY, "Battery: --%", false, None);
    menu.append(&battery_item).unwrap();

    let quit_item = tray_icon::menu::MenuItem::with_id(ID_QUIT, "Quit", true, None);
    menu.append(&quit_item).unwrap();

    let _tray_icon = Some(
        TrayIconBuilder::new()
            .with_menu(Box::new(menu))
            .with_tooltip("Think Rider")
            .with_icon(icon)
            .build()
            .unwrap(),
    );

    let _ = event_loop.run(move |event, event_loop| {
        event_loop.set_control_flow(ControlFlow::Wait);
        if let WinitEvent::AboutToWait = event {
            // 1. Process updates from the Worker
            while let Ok(worker_event) = event_rx.try_recv() {
                match worker_event {
                    WorkerEvent::StatusUpdate(ref new_state) => {
                        let status_text = match new_state {
                            State::Disconnected => "Status: Disconnected".to_string(),
                            State::Searching => "Status: Searching...".to_string(),
                            State::Connected { device_address } => {
                                format!("Status: Connected ({})", device_address)
                            }
                        };
                        status_item.set_text(status_text);
                        search_item.set_enabled(matches!(new_state, State::Disconnected));
                        stop_search_item.set_enabled(matches!(new_state, State::Searching));
                        battery_item.set_enabled(matches!(new_state, State::Connected { .. }));
                        if !matches!(new_state, State::Connected { .. }) {
                            battery_item.set_text("Battery: --%");
                        }
                    }
                    WorkerEvent::BatteryLevel(level) => {
                        battery_item.set_text(&format!("Battery: {}%", level));
                    }
                    WorkerEvent::Log(msg) => {
                        if app_state.notifications_enabled {
                            let _ = notify_rust::Notification::new()
                                .summary("Think Rider")
                                .body(&msg)
                                .show();
                        }
                    }
                    _ => {}
                }
            }

            // 2. Poll systray events (unused currently but kept for structure)
            while let Ok(_tray_event) = TrayIconEvent::receiver().try_recv() {}

            // 3. Process menu interactions
            while let Ok(menu_event) = MenuEvent::receiver().try_recv() {
                let id = menu_event.id();
                if *id == MenuId(ID_NOTIFY.to_string()) {
                    app_state.notifications_enabled = !app_state.notifications_enabled;
                    let _ = notify_tx.send(app_state.notifications_enabled);
                    notify_item.set_text(if app_state.notifications_enabled { "Notification [ON]" } else { "Notification [OFF]" });
                } else if *id == MenuId(ID_SEARCH.to_string()) {
                    let _ = cmd_tx.send(Command::SearchDevice);
                } else if *id == MenuId(ID_STOP_SEARCH.to_string()) {
                    let _ = cmd_tx.send(Command::StopSearch);
                } else if *id == MenuId(ID_QUIT.to_string()) {
                    std::process::exit(0);
                }
            }
        }
    });
}
