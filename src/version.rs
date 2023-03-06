#[macro_export]
macro_rules! tools_version {
    () => {
        env!("CARGO_PKG_VERSION")
    };
}
