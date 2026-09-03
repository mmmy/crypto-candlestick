use std::{fs, path::Path};
use tracing_appender::non_blocking::WorkerGuard;
use tracing_subscriber::{layer::SubscriberExt, util::SubscriberInitExt, EnvFilter};

pub fn init(log_dir: impl AsRef<Path>, log_level: &str) -> Result<WorkerGuard, std::io::Error> {
    let log_dir = log_dir.as_ref();
    fs::create_dir_all(log_dir)?;

    let file_appender = tracing_appender::rolling::daily(log_dir, "crypto-candlestick.log");
    let (file_writer, guard) = tracing_appender::non_blocking(file_appender);
    let stdout_layer = tracing_subscriber::fmt::layer().with_writer(std::io::stdout);
    let file_layer = tracing_subscriber::fmt::layer()
        .with_writer(file_writer)
        .with_ansi(false);

    let log_filter = EnvFilter::try_new(log_level)
        .map_err(|error| std::io::Error::new(std::io::ErrorKind::InvalidInput, error))?;

    tracing_subscriber::registry()
        .with(log_filter)
        .with(stdout_layer)
        .with(file_layer)
        .init();

    Ok(guard)
}
