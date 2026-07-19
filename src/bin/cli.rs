use std::env;
use thinkrider_vs200_rs::{
    BleCommand, BleConfig, BleEvent, BleSystem, BleSystemHandle, InteractionType,
};
use tokio::sync::broadcast;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    tracing_subscriber::fmt::init();

    let args: Vec<String> = env::args().collect();
    let mut config = BleConfig::default();

    for arg in &args {
        if arg.to_lowercase() == "debug" {
            config.debug = true;
        }
    }

    println!("Starting Think Rider CLI...");
    if config.debug {
        println!("Debug mode enabled");
    }

    // Bootstrap the entire BLE subsystem (Worker + Interaction Manager)
    let system = BleSystem::new(config.clone());
    let BleSystemHandle {
        command_sender,
        event_sender,
        ..
    } = system.handle();

    system
        .bootstrap(
            thinkrider_vs200_rs::ThinkRiderVS200,
            InteractionType::Keyboard,
        )
        .await?;

    // Trigger initial search
    let _ = command_sender.send(BleCommand::SearchDevice);

    println!("Listening for events... (Press Ctrl+C to stop)");

    loop {
        let mut event_receiver = event_sender.subscribe();
        match event_receiver.recv().await {
            Ok(event) => match event {
                BleEvent::StatusUpdate(state) => {
                    println!("Status: {}", state);
                }
                BleEvent::BatteryLevel(level) => {
                    println!("Battery level: {}%", level);
                    if level <= 20 {
                        println!("Low battery warning!");
                    }
                }
                BleEvent::KeyPress(key) => {
                    println!("Key Pressed: {}", key);
                }
            },
            Err(broadcast::error::RecvError::Lagged(n)) => {
                tracing::warn!(lagged_messages = n, "Receiver lagged");
            }
            Err(broadcast::error::RecvError::Closed) => {
                tracing::error!("Event channel closed. Exiting...");
                break;
            }
        }
    }

    Ok(())
}
