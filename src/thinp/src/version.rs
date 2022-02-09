static TOOLS_VERSION: &str = include_str!("../../../VERSION");

#[macro_export]
macro_rules! tools_version {
    () => {
        include_str!("../../../VERSION");
    };
}

pub fn tools_version() -> &'static str {
    TOOLS_VERSION.trim_end()
}
