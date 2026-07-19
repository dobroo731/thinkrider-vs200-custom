use thinkrider_vs200_rs::{BleCommand, BleConfig, BleEvent, BleSystem, InteractionType, State};
use tokio::sync::{mpsc, watch};
use tray_icon::menu::{MenuEvent, MenuId, MenuItem};
use tray_icon::{Icon, TrayIconBuilder, TrayIconEvent};
use winit::{
    application::ApplicationHandler,
    event_loop::{ActiveEventLoop, ControlFlow, EventLoop},
};

enum UserEvent {
    BleUpdate(BleEvent),
    Tray(TrayIconEvent),
    Menu(MenuEvent),
    ConfigChanged(BleConfig),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, strum::Display, strum::EnumString, strum::EnumIter)]
pub enum MenuAction {
    ToggleNotify,
    Search,
    StopSearch,
    Status,
    Battery,
    Quit,
}

impl TryFrom<MenuId> for MenuAction {
    type Error = strum::ParseError;
    fn try_from(id: MenuId) -> Result<Self, Self::Error> {
        use std::str::FromStr;
        Self::from_str(id.as_ref())
    }
}

struct MenuItems {
    status: MenuItem,
    search: MenuItem,
    stop_search: MenuItem,
    battery: MenuItem,
    notify: MenuItem,
}

struct TrayApp {
    command_sender: mpsc::UnboundedSender<BleCommand>,
    config_receiver: watch::Sender<BleConfig>,
    items: MenuItems,
}

impl TrayApp {
    fn send_notification(&self, body: &str) {
        if self.config_receiver.borrow().notifications_enabled {
            let _ = notify_rust::Notification::new()
                .summary("Think Rider")
                .body(body)
                .show();
        }
    }
}

impl ApplicationHandler<UserEvent> for TrayApp {
    fn resumed(&mut self, event_loop: &ActiveEventLoop) {
        event_loop.set_control_flow(ControlFlow::Wait);
    }

    fn window_event(
        &mut self,
        _event_loop: &ActiveEventLoop,
        _id: winit::window::WindowId,
        _event: winit::event::WindowEvent,
    ) {
        // No windows in this app
    }

    fn user_event(&mut self, _event_loop: &ActiveEventLoop, event: UserEvent) {
        match event {
            UserEvent::BleUpdate(worker_event) => match worker_event {
                BleEvent::StatusUpdate(ref new_state) => {
                    let status_text = match new_state {
                        State::Disconnected => "Status: Disconnected".to_string(),
                        State::Searching => "Status: Searching...".to_string(),
                        State::Connecting { device_address } => {
                            format!("Status: Connecting to {}...", device_address)
                        }
                        State::Connected { device_address } => {
                            format!("Status: Connected ({})", device_address)
                        }
                        State::Reconnecting { device_address } => {
                            format!("Status: Reconnecting to {}...", device_address)
                        }
                    };
                    self.items.status.set_text(status_text);
                    self.items
                        .search
                        .set_enabled(matches!(new_state, State::Disconnected));
                    self.items
                        .stop_search
                        .set_enabled(matches!(new_state, State::Searching));
                    self.items
                        .battery
                        .set_enabled(matches!(new_state, State::Connected { .. }));
                    if !matches!(new_state, State::Connected { .. }) {
                        self.items.battery.set_text("Battery: --%");
                    }

                    if let Some(msg) = match new_state {
                        State::Connected { device_address } => {
                            Some(format!("Connected to {}", device_address))
                        }
                        State::Disconnected => Some("Disconnected".to_string()),
                        _ => None,
                    } {
                        self.send_notification(&msg);
                    }
                }
                BleEvent::BatteryLevel(level) => {
                    self.items.battery.set_text(format!("Battery: {}%", level));
                }
                BleEvent::KeyPress(key) => {
                    self.send_notification(&format!("Key Pressed: {}", key));
                }
            },
            UserEvent::Tray(_tray_event) => {
                // Handle tray icon events (e.g., clicks) if needed in the future
            }
            UserEvent::Menu(menu_event) => {
                let id = menu_event.id().clone();
                if let Ok(action) = MenuAction::try_from(id) {
                    match action {
                        MenuAction::ToggleNotify => {
                            let mut current = self.config_receiver.borrow().clone();
                            current.notifications_enabled = !current.notifications_enabled;
                            let _ = self.config_receiver.send(current);
                        }
                        MenuAction::Search => {
                            let _ = self.command_sender.send(BleCommand::SearchDevice);
                        }
                        MenuAction::StopSearch => {
                            let _ = self.command_sender.send(BleCommand::StopSearch);
                        }
                        MenuAction::Quit => {
                            std::process::exit(0);
                        }
                        _ => {}
                    }
                }
            }
            UserEvent::ConfigChanged(config) => {
                self.items.notify.set_text(if config.notifications_enabled {
                    "Notification [ON]"
                } else {
                    "Notification [OFF]"
                });
            }
        }
    }
}

impl TrayMenu {
    fn new() -> Result<Self, Box<dyn std::error::Error>> {
        let menu = tray_icon::menu::Menu::new();

        let notify = MenuItem::with_id(MenuAction::ToggleNotify, "Notification [ON]", true, None);
        let search = MenuItem::with_id(MenuAction::Search, "Search Device", true, None);
        let stop_search = MenuItem::with_id(MenuAction::StopSearch, "Stop Searching", false, None);
        let status = MenuItem::with_id(MenuAction::Status, "Status: Disconnected", false, None);
        let battery = MenuItem::with_id(MenuAction::Battery, "Battery: --%", false, None);
        let quit = MenuItem::with_id(MenuAction::Quit, "Quit", true, None);

        menu.append(&notify)?;
        menu.append(&search)?;
        menu.append(&stop_search)?;
        menu.append(&status)?;
        menu.append(&battery)?;
        menu.append(&quit)?;

        Ok(Self {
            menu,
            items: MenuItems {
                status,
                search,
                stop_search,
                battery,
                notify,
            },
        })
    }
}

struct TrayMenu {
    menu: tray_icon::menu::Menu,
    items: MenuItems,
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    tracing_subscriber::fmt::init();

    let event_loop = EventLoop::<UserEvent>::with_user_event().build()?;
    let proxy = event_loop.create_proxy();

    let proxy_tray = proxy.clone();
    TrayIconEvent::set_event_handler(Some(move |event| {
        let _ = proxy_tray.send_event(UserEvent::Tray(event));
    }));

    let proxy_menu = proxy.clone();
    MenuEvent::set_event_handler(Some(move |event| {
        let _ = proxy_menu.send_event(UserEvent::Menu(event));
    }));

    let app_state = BleConfig {
        notifications_enabled: true,
        ..Default::default()
    };

    // Use the bootstrapper to initialize BLE subsystem
    let rt = tokio::runtime::Runtime::new()?;
    let (command_sender, mut event_receiver, config_sender, config_receiver) =
        rt.block_on(async {
            BleSystem::new(app_state)
                .bootstrap(
                    thinkrider_vs200_rs::ThinkRiderVS200,
                    InteractionType::Keyboard,
                )
                .await
        })?;

    let proxy_clone = proxy.clone();
    rt.spawn(async move {
        loop {
            match event_receiver.recv().await {
                Ok(event) => {
                    let _ = proxy_clone.send_event(UserEvent::BleUpdate(event));
                }
                Err(tokio::sync::broadcast::error::RecvError::Lagged(_)) => {
                    tracing::warn!("Tray Bridge lagged");
                }
                Err(tokio::sync::broadcast::error::RecvError::Closed) => break,
            }
        }
    });

    let mut watcher_config_receiver = config_receiver;
    let proxy_watcher = proxy.clone();
    rt.spawn(async move {
        while watcher_config_receiver.changed().await.is_ok() {
            let new_config = watcher_config_receiver.borrow().clone();
            let _ = proxy_watcher.send_event(UserEvent::ConfigChanged(new_config));
        }
    });

    let icon_data = vec![255u8; 32 * 32 * 4];
    let icon = Icon::from_rgba(icon_data, 32, 32)?;
    let tray_menu = TrayMenu::new()?;

    TrayIconBuilder::new()
        .with_menu(Box::new(tray_menu.menu))
        .with_tooltip("Think Rider")
        .with_icon(icon)
        .build()?;

    let mut app = TrayApp {
        command_sender,
        config_receiver: config_sender,
        items: tray_menu.items,
    };

    event_loop.run_app(&mut app)?;

    Ok(())
}
