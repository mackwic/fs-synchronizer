use fern::colors::{Color, ColoredLevelConfig};
use log::debug;

pub fn setup_logs(is_debug: bool) {
    let colors = ColoredLevelConfig::new().error(Color::Red);

    let base_config = if is_debug {
        fern::Dispatch::new().level(log::LevelFilter::Debug)
    } else {
        fern::Dispatch::new().level(log::LevelFilter::Info)
    };

    base_config
        .chain(std::io::stdout())
        .format(move |out, message, record| {
            out.finish(format_args!(
                "[{}]{} {}",
                // This will color the log level only, not the whole line. Just a touch.
                colors.color(record.level()),
                chrono::Utc::now().format("[%Y-%m-%d %H:%M:%S.%3f %z]"),
                message
            ))
        })
        .apply()
        .expect("Unable to set logs !");

    debug!("logs set !")
}
