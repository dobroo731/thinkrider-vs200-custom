use btleplug::api::{Central, Manager as _, Peripheral, ScanFilter};
use btleplug::platform::{Manager};
use enigo::{Enigo, Keyboard, Key, Settings, Direction};
use std::env;
use std::error::Error;
use tokio::time;
use futures::stream::StreamExt;

const DEVICE_NAME_FILTER: &str = "Think VS01-";
const NOTIFY_UUID_STR: &str = "0000fea1-0000-1000-8000-00805f9b34fb";
const SHIFT_UP_PATTERN: &[u8] = &[0xf3, 0x05, 0x03, 0x01, 0xfc];
const SHIFT_DOWN_PATTERN: &[u8] = &[0xf3, 0x05, 0x03, 0x00, 0xfb];
const BATTERY_LEVEL_UUID_STR: &str = "00002a19-0000-1000-8000-00805f9b34fb";

struct Config {
    up_key: Key,
    down_key: Key,
    debug: bool,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    let args: Vec<String> = env::args().collect();
    let mut config = Config {
        up_key: Key::K,
        down_key: Key::I,
        debug: false,
    };

    for arg in &args {
        if arg.to_lowercase() == "rouvy" {
            // Note: Using K/I as placeholders since symbol variants
            // vary across OS for enigo v0.6.
            config.up_key = Key::K;
            config.down_key = Key::I;
        } else if arg.to_lowercase() == "debug" {
            config.debug = true;
        }
    }

    let manager = Manager::new().await?;
    let adapters = manager.adapters().await?;
    let adapter = adapters.into_iter().next().ok_or("No Bluetooth adapter found")?;

    println!("🔍 Scanning for {}... (waiting for connection)", DEVICE_NAME_FILTER);
    adapter.start_scan(ScanFilter::default()).await?;

    let mut device = None;
    let timeout = time::Duration::from_secs(20);
    let start_time = std::time::Instant::now();

    while start_time.elapsed() < timeout {
        let peripherals = adapter.peripherals().await?;
        for p in peripherals {
            if let Some(props) = p.properties().await?
                && let Some(name) = props.local_name
                && name.contains(DEVICE_NAME_FILTER)
            {
                device = Some(p);
                break;
            }
        }
        if device.is_some() { break; }
        time::sleep(time::Duration::from_millis(500)).await;
    }

    let peripheral = match device {
        Some(p) => p,
        None => return Err(format!("❌ Device '{}' not found.", DEVICE_NAME_FILTER).into()),
    };

    let props = peripheral.properties().await?.unwrap();
    println!("✅ Found device: {} ({:?})", props.local_name.unwrap_or_default(), peripheral.address());

    peripheral.connect().await?;
    println!("🔗 Connected!");

    peripheral.discover_services().await?;

    let chars = peripheral.characteristics();
    if let Some(battery_char) = chars.iter().find(|c| c.uuid.to_string() == BATTERY_LEVEL_UUID_STR) {
        let data = peripheral.read(battery_char).await?;
        if !data.is_empty() {
            let level = data[0];
            println!("🔋 Battery level: {}%", level);
            if level <= 20 {
                println!("⚠️ Low battery!");
            }
        }
    }

    let notify_char = chars.iter().find(|c| c.uuid.to_string() == NOTIFY_UUID_STR)
        .ok_or("Could not find notification characteristic")?;

    peripheral.subscribe(notify_char).await?;
    println!("📡 Subscribed to {} notifications", NOTIFY_UUID_STR);

    let mut notifications = peripheral.notifications().await?;
    let mut enigo = Enigo::new(&Settings::default()).unwrap();

    println!("🎧 Listening for shift signals... (Press Ctrl+C to stop)");

    while let Some(notification) = notifications.next().await {
        if config.debug {
            println!("📨 Data: {:02x?}", notification.value);
        }

        if notification.value == SHIFT_UP_PATTERN {
            if config.debug { println!("👋 Pressed: Up"); }
            let _ = enigo.key(config.up_key, Direction::Press);
            let _ = enigo.key(config.up_key, Direction::Release);
        } else if notification.value == SHIFT_DOWN_PATTERN {
            if config.debug { println!("👋 Pressed: Down"); }
            let _ = enigo.key(config.down_key, Direction::Press);
            let _ = enigo.key(config.down_key, Direction::Release);
        }
    }

    Ok(())
}
