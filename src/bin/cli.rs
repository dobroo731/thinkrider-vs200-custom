use thinkrider_vs200_rs::{BleCommand, BleEvent, BleSystem, BleSystemHandle};

use tokio::sync::broadcast;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    tracing_subscriber::FmtSubscriber::builder()
        .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
        .with_ansi_sanitization(false)
        .init();

    println!("Starting Think Rider CLI...");

    // Bootstrap the entire BLE subsystem (Worker + Interaction Manager)
    let system = BleSystem::default();
    let BleSystemHandle {
        command_sender,
        event_sender,
        ..
    } = system.handle();

    system
        .bootstrap(
            thinkrider_vs200_rs::ThinkRiderVS200,
            thinkrider_vs200_rs::KeyboardInteraction::new()?,
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
