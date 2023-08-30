/* Rust port of kernel include/uapi/asm-generic/ioctl.h */

#[cfg(test)]
mod tests;

//------------------------------------------

#[cfg(target_env = "musl")]
pub type RequestType = libc::c_int;
#[cfg(not(target_env = "musl"))]
pub type RequestType = libc::c_ulong;

#[cfg(any(
    target_arch = "mips",
    target_arch = "mips64",
    target_arch = "powerpc",
    target_arch = "powerpc64",
    target_arch = "powerpc64le",
    target_arch = "sparc",
    target_arch = "sparc64"
))]
mod defs {
    use super::RequestType;
    pub const IOC_NONE: RequestType = 1;
    pub const IOC_READ: RequestType = 2;
    pub const IOC_WRITE: RequestType = 4;
    pub const IOC_DIRBITS: RequestType = 3;
    pub const IOC_SIZEBITS: RequestType = 13;
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
mod defs {
    use super::RequestType;
    pub const IOC_NONE: RequestType = 0;
    pub const IOC_WRITE: RequestType = 1;
    pub const IOC_READ: RequestType = 2;
    pub const IOC_DIRBITS: RequestType = 2;
    pub const IOC_SIZEBITS: RequestType = 14;
}

pub use defs::*;

pub const IOC_NRBITS: RequestType = 8;
pub const IOC_TYPEBITS: RequestType = 8;

pub const IOC_NRMASK: RequestType = (1 << IOC_NRBITS) - 1;
pub const IOC_TYPEMASK: RequestType = (1 << IOC_TYPEBITS) - 1;
pub const IOC_SIZEMASK: RequestType = (1 << IOC_SIZEBITS) - 1;
pub const IOC_DIRMASK: RequestType = (1 << IOC_DIRBITS) - 1;

pub const IOC_NRSHIFT: RequestType = 0;
pub const IOC_TYPESHIFT: RequestType = IOC_NRSHIFT + IOC_NRBITS;
pub const IOC_SIZESHIFT: RequestType = IOC_TYPESHIFT + IOC_TYPEBITS;
pub const IOC_DIRSHIFT: RequestType = IOC_SIZESHIFT + IOC_SIZEBITS;

#[macro_export]
macro_rules! ioc {
    ($dir: expr, $typ: expr, $nr: expr, $size: expr) => {
        (($dir as RequestType & IOC_DIRMASK) << IOC_DIRSHIFT)
            | (($typ as RequestType & IOC_TYPEMASK) << IOC_TYPESHIFT)
            | (($nr as RequestType & IOC_NRMASK) << IOC_NRSHIFT)
            | (($size as RequestType & IOC_SIZEMASK) << IOC_SIZESHIFT)
    };
}

//------------------------------------------

#[macro_export]
macro_rules! request_code_none {
    ($typ: expr, $nr: expr) => {
        $crate::ioc!(IOC_NONE, $typ, $nr, 0)
    };
}

#[macro_export]
macro_rules! request_code_read {
    ($typ: expr, $nr: expr, $size_type: ty) => {
        $crate::ioc!(IOC_READ, $typ, $nr, ::std::mem::size_of::<$size_type>())
    };
}

#[macro_export]
macro_rules! request_code_write {
    ($typ: expr, $nr: expr, $size_type: ty) => {
        $crate::ioc!(IOC_WRITE, $typ, $nr, ::std::mem::size_of::<$size_type>())
    };
}

#[macro_export]
macro_rules! request_code_readwrite {
    ($typ: expr, $nr: expr, $size_type: ty) => {
        $crate::ioc!(
            IOC_READ | IOC_WRITE,
            $typ,
            $nr,
            ::std::mem::size_of::<$size_type>()
        )
    };
}

//------------------------------------------
