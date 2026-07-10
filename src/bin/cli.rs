use thinkrider_vs200_rs::{BleConfig, BleCommand, BleEvent, State, run_ble_worker};
use std::env;
use tokio::sync::{mpsc, watch};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args: Vec<String> = env::args().collect();
    let mut config = BleConfig::default();

    for arg in &args {
        if arg.to_lowercase() == "debug" {
            config.debug = true;
        }
    }

    let (cmd_tx, cmd_rx) = mpsc::unbounded_channel();
    let (event_tx, mut event_rx) = mpsc::unbounded_channel();
    let (_notify_tx, notify_rx) = watch::channel(config.notifications_enabled);

    println!("🚀 Starting Think Rider CLI...");
    if config.debug {
        println!("🛠️ Debug mode enabled");
    }

    // Start the BLE worker in a separate task
    let worker_config = BleConfig {
        up_key: config.up_key,
        down_key: config.down_key,
        debug: config.debug,
        notifications_enabled: config.notifications_enabled,
    };

    tokio::spawn(async move {
        if let Err(e) = run_ble_worker(worker_config, event_tx, cmd_rx, notify_rx).await {
            eprintln!("❌ BLE Worker Error: {}", e);
        }
    });

    // Trigger initial search
    let _ = cmd_tx.send(BleCommand::SearchDevice);

    println!("🎧 Listening for events... (Press Ctrl+C to stop)");

    while let Some(event) = event_rx.recv().await {
        match event {
            BleEvent::StatusUpdate(state) => {
                match state {
                    State::Disconnected => println!("📡 Status: Disconnected"),
                    State::Searching => println!("🔍 Scanning for device..."),
                    State::Connecting { device_address } => println!("⏳ Connecting to {}...", device_address),
                    State::Connected { device_address } => {
                        println!("✅ Connected to device: {}", device_address);
                    }
                    State::Reconnecting { device_address } => println!("🔄 Reconnecting to {}...", device_address),
                }
            }
            BleEvent::Log(msg) => {
                if config.debug {
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
        }
    }

    Ok(())
}
