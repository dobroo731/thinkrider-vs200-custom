use thinkrider_vs200_rs::{BleCommand, BleEvent, BleSystem, BleSystemHandle, State};
use tokio::sync::mpsc;
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
    items: MenuItems,
    notify: bool,
}

impl TrayApp {
    fn send_notification(&self, body: &str) {
        if self.notify {
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

    fn user_event(&mut self, event_loop: &ActiveEventLoop, event: UserEvent) {
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
                            self.notify = !self.notify;
                            self.items.notify.set_text(if self.notify {
                                "Notification [ON]"
                            } else {
                                "Notification [OFF]"
                            });
                        }
                        MenuAction::Search => {
                            let _ = self.command_sender.send(BleCommand::SearchDevice);
                        }
                        MenuAction::StopSearch => {
                            let _ = self.command_sender.send(BleCommand::StopSearch);
                        }
                        MenuAction::Quit => {
                            event_loop.exit();
                        }
                        _ => {}
                    }
                }
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
    tracing_subscriber::FmtSubscriber::builder()
        .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
        .with_ansi_sanitization(false)
        .init();

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

    // 1. Initialize BLE System synchronously (creates channels only)
    let system = BleSystem::default();
    let BleSystemHandle {
        command_sender,
        event_sender,
    } = system.handle();

    // 2. Setup Runtime and start BLE in background
    let tokio = tokio::runtime::Runtime::new()?;

    let handler = thinkrider_vs200_rs::KeyboardInteraction::new()?;
    let background_task = tokio.block_on(async {
        system
            .bootstrap(thinkrider_vs200_rs::ThinkRiderVS200, handler)
            .await
    })?;

    // 3. Setup Bridge Tasks immediately
    let proxy_clone = proxy.clone();
    tokio.spawn(async move {
        let mut event_receiver = event_sender.subscribe();
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

    let icon_data = vec![255u8; 32 * 32 * 4];
    let icon = Icon::from_rgba(icon_data, 32, 32)?;
    let tray_menu = TrayMenu::new()?;

    let tray_icon = TrayIconBuilder::new()
        .with_menu(Box::new(tray_menu.menu))
        .with_tooltip("Think Rider")
        .with_icon(icon)
        .build()?;

    let mut app = TrayApp {
        command_sender,
        items: tray_menu.items,
        notify: true,
    };

    // Trigger auto-search on startup to initiate connection process immediately
    let _ = app.command_sender.send(BleCommand::SearchDevice);

    event_loop.run_app(&mut app)?;

    // extend lifecycles till app close to prevent early resource cleanup
    drop(tray_icon);
    drop(background_task);
    Ok(())
}
