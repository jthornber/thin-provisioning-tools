use crate::ioctl::*;

//------------------------------------------

#[cfg(any(
    target_arch = "mips",
    target_arch = "mips64",
    target_arch = "powerpc",
    target_arch = "powerpc64",
    target_arch = "powerpc64le",
    target_arch = "sparc",
    target_arch = "sparc64"
))]
mod expected {
    use super::RequestType;
    pub const BLKDISCARD: RequestType = 0x20001277;

    #[cfg(target_pointer_width = "32")]
    mod sized {
        use super::RequestType;
        pub const BLKBSZSET: RequestType = 0x80041271;
        pub const BLKGETSIZE64: RequestType = 0x40041272;
    }

    #[cfg(target_pointer_width = "64")]
    mod sized {
        use super::RequestType;
        pub const BLKBSZSET: RequestType = 0x80081271;
        pub const BLKGETSIZE64: RequestType = 0x40081272;
    }

    pub use sized::*;
}

#[cfg(not(any(
    target_arch = "mips",
    target_arch = "mips64",
    target_arch = "powerpc",
    target_arch = "powerpc64",
    target_arch = "powerpc64le",
    target_arch = "sparc",
    target_arch = "sparc64"
)))]
mod expected {
    use super::RequestType;
    pub const BLKDISCARD: RequestType = 0x1277;

    #[cfg(target_pointer_width = "32")]
    mod sized {
        use super::RequestType;
        pub const BLKBSZSET: RequestType = 0x40041271;
        pub const BLKGETSIZE64: RequestType = 0x80041272;
    }

    #[cfg(target_pointer_width = "64")]
    mod sized {
        use super::RequestType;
        pub const BLKBSZSET: RequestType = 0x40081271;
        pub const BLKGETSIZE64: RequestType = 0x80081272;
    }

    pub use sized::*;
}

#[test]
fn test_ioc_none() {
    assert_eq!(crate::request_code_none!(0x12, 119), expected::BLKDISCARD);
}

#[test]
fn test_ioc_read_usize() {
    assert_eq!(
        crate::request_code_read!(0x12, 114, usize),
        expected::BLKGETSIZE64
    );
}

#[test]
fn test_ioc_write_usize() {
    assert_eq!(
        crate::request_code_write!(0x12, 113, usize),
        expected::BLKBSZSET
    );
}

//------------------------------------------
