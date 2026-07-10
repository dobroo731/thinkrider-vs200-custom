use thinkrider_vs200_rs::{BleConfig, BleCommand, BleEvent, State, run_ble_worker};
use tray_icon::{TrayIconBuilder, TrayIconEvent, Icon};
use tray_icon::menu::{MenuEvent, MenuId};
use winit::{
    event::Event as WinitEvent,
    event_loop::{ControlFlow, EventLoop},
};
use tokio::sync::{mpsc, watch};

fn main() {
    let event_loop = EventLoop::new().unwrap();

    let mut app_state = BleConfig {
        notifications_enabled: true,
        ..Default::default()
    };

    let (cmd_tx, cmd_rx) = mpsc::unbounded_channel();
    let (event_tx, mut event_rx) = mpsc::unbounded_channel();
    let (notify_tx, notify_rx) = watch::channel(app_state.notifications_enabled);

    std::thread::spawn(move || {
        let rt = tokio::runtime::Runtime::new().unwrap();
        if let Err(e) = rt.block_on(run_ble_worker(BleConfig::default(), event_tx, cmd_rx, notify_rx)) {
            eprintln!("BLE worker error: {}", e);
        }
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
            while let Ok(worker_event) = event_rx.try_recv() {
                match worker_event {
                    BleEvent::StatusUpdate(ref new_state) => {
                        let status_text = match new_state {
                            State::Disconnected => "Status: Disconnected".to_string(),
                            State::Searching => "Status: Searching...".to_string(),
                            State::Connecting { device_address } => format!("Status: Connecting to {}...", device_address),
                            State::Connected { device_address } => format!("Status: Connected ({})", device_address),
                            State::Reconnecting { device_address } => format!("Status: Reconnecting to {}...", device_address),
                        };
                        status_item.set_text(status_text);
                        search_item.set_enabled(matches!(new_state, State::Disconnected));
                        stop_search_item.set_enabled(matches!(new_state, State::Searching));
                        battery_item.set_enabled(matches!(new_state, State::Connected { .. }));
                        if !matches!(new_state, State::Connected { .. }) {
                            battery_item.set_text("Battery: --%");
                        }

                        if app_state.notifications_enabled {
                            match new_state {
                                State::Connected { device_address } => {
                                    let _ = notify_rust::Notification::new()
                                        .summary("Think Rider")
                                        .body(&format!("Connected to {}", device_address))
                                        .show();
                                }
                                State::Disconnected => {
                                    let _ = notify_rust::Notification::new()
                                        .summary("Think Rider")
                                        .body("Disconnected")
                                        .show();
                                }
                                _ => {}
                            }
                        }
                    }
                    BleEvent::BatteryLevel(level) => {
                        battery_item.set_text(format!("Battery: {}%", level));
                    }
                    BleEvent::KeyPress(key) => {
                        if app_state.notifications_enabled {
                            let _ = notify_rust::Notification::new()
                                .summary("Think Rider")
                                .body(&format!("Key Pressed: {}", key))
                                .show();
                        }
                    }
                    BleEvent::Log(_msg) => {}
                }
            }

            while let Ok(_tray_event) = TrayIconEvent::receiver().try_recv() {}

            while let Ok(menu_event) = MenuEvent::receiver().try_recv() {
                let id = menu_event.id();
                if *id == MenuId(ID_NOTIFY.to_string()) {
                    app_state.notifications_enabled = !app_state.notifications_enabled;
                    let _ = notify_tx.send(app_state.notifications_enabled);
                    notify_item.set_text(if app_state.notifications_enabled { "Notification [ON]" } else { "Notification [OFF]" });
                } else if *id == MenuId(ID_SEARCH.to_string()) {
                    let _ = cmd_tx.send(BleCommand::SearchDevice);
                } else if *id == MenuId(ID_STOP_SEARCH.to_string()) {
                    let _ = cmd_tx.send(BleCommand::StopSearch);
                } else if *id == MenuId(ID_QUIT.to_string()) {
                    std::process::exit(0);
                }
            }
        }
    });
}
