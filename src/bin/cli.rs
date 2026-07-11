use std::env;
use thinkrider_vs200_rs::{BleCommand, BleConfig, BleEvent, BleWorker};
use tokio::sync::{broadcast, mpsc, watch};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let args: Vec<String> = env::args().collect();
    let mut config = BleConfig::default();

    for arg in &args {
        if arg.to_lowercase() == "debug" {
            config.debug = true;
        }
    }

    println!("🚀 Starting Think Rider CLI...");
    if config.debug {
        println!("🛠️ Debug mode enabled");
    }

    let (cmd_tx, cmd_rx) = mpsc::unbounded_channel();
    let (event_tx, mut event_rx) = broadcast::channel(16);
    let (_config_tx, config_rx) = watch::channel(config);

    // Initialize the interaction manager with default keyboard behavior.
    let manager = thinkrider_vs200_rs::InteractionManager::new_keyboard(
        event_tx.subscribe(),
        config_rx.clone(),
    )?;
    tokio::spawn(async move {
        manager.run().await;
    });

    let worker_config_rx = config_rx.clone();
    let event_tx_clone = event_tx.clone();
    tokio::spawn(async move {
        let worker = match BleWorker::new(worker_config_rx, event_tx_clone, cmd_rx).await {
            Ok(w) => w,
            Err(e) => {
                eprintln!("❌ Failed to initialize BLE Worker: {}", e);
                return;
            }
        };

        if let Err(e) = worker.run().await {
            eprintln!("❌ BLE Worker Error: {}", e);
        }
    });

    // Trigger initial search
    let _ = cmd_tx.send(BleCommand::SearchDevice);

    println!("🎧 Listening for events... (Press Ctrl+C to stop)");

    loop {
        match event_rx.recv().await {
            Ok(event) => match event {
                BleEvent::StatusUpdate(state) => {
                    println!("📡 Status: {}", state);
                }
                BleEvent::Log(msg) => {
                    if config_rx.borrow().debug {
                        println!("📝 Log: {}", msg);
                    }
                }
                BleEvent::BatteryLevel(level) => {
                    println!("🔋 Battery level: {}%", level);
                    if level <= 20 {
                        println!("⚠️ Low battery warning!");
                    }
                }
                BleEvent::KeyPress(key) => {
                    println!("⌨️ Key Pressed: {}", key);
                }
            },
            Err(broadcast::error::RecvError::Lagged(n)) => {
                eprintln!("⚠️  Warning: receiver lagged, missed {} messages", n);
            }
            Err(broadcast::error::RecvError::Closed) => {
                eprintln!("❌ Event channel closed. Exiting...");
                break;
            }
        }
    }

    Ok(())
}
