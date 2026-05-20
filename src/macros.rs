pub(crate) fn timestamp() -> String {
    static FORMAT: std::sync::OnceLock<Vec<time::format_description::BorrowedFormatItem<'static>>> =
        std::sync::OnceLock::new();

    let now = time::OffsetDateTime::now_local().unwrap_or_else(|_| time::OffsetDateTime::now_utc());
    let format = FORMAT.get_or_init(|| {
        time::format_description::parse("[hour]:[minute]:[second].[subsecond digits:3]")
            .unwrap_or_default()
    });

    now.format(format)
        .unwrap_or_else(|_| "00:00:00.000".to_string())
}

#[macro_export]
macro_rules! get_timestamp {
    () => {{ $crate::macros::timestamp() }};
}

#[macro_export]
macro_rules! info {
    ($($arg:tt)*) => {{
        let timestamp = $crate::get_timestamp!();
        println!("{} INFO {}", timestamp, format_args!($($arg)*));
    }};
}

#[macro_export]
macro_rules! err {
    ($($arg:tt)*) => {{
        let timestamp = $crate::get_timestamp!();
        eprintln!("{} ERROR {}", timestamp, format_args!($($arg)*));
    }};
}

#[macro_export]
macro_rules! warn {
    ($($arg:tt)*) => {{
        let timestamp = $crate::get_timestamp!();
        eprintln!("{} WARN {}", timestamp, format_args!($($arg)*));
    }};
}

#[macro_export]
macro_rules! trace {
    ($($arg:tt)*) => {{
        if *LOG_TRACE.get().unwrap_or(&false) {
            let timestamp = $crate::get_timestamp!();
            println!("{} TRACE {}", timestamp, format_args!($($arg)*));
        }
    }};
}
