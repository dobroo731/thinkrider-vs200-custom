use std::env;
use thinkrider_vs200_rs::{BleCommand, BleConfig, BleEvent, run_ble_worker};
use tokio::sync::{broadcast, mpsc, watch};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
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

    let worker_config_rx = config_rx.clone();
    tokio::spawn(async move {
        if let Err(e) = run_ble_worker(worker_config_rx, event_tx, cmd_rx).await {
            eprintln!("❌ BLE Worker Error: {}", e);
        }
    });

    // Trigger initial search
    let _ = cmd_tx.send(BleCommand::SearchDevice);

    println!("🎧 Listening for events... (Press Ctrl+C to stop)");

    while let Ok(event) = event_rx.recv().await {
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
        }
    }

    Ok(())
}
