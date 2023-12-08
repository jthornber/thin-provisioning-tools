use anyhow::anyhow;
use std::str::FromStr;

//------------------------------------------

#[derive(Copy, Clone, Debug, PartialEq, Eq)]
pub enum Units {
    Byte,
    Sector,
    Kilobyte,
    Megabyte,
    Gigabyte,
    Terabyte,
    Petabyte,
    Exabyte,
    Kibibyte,
    Mebibyte,
    Gibibyte,
    Tebibyte,
    Pebibyte,
    Exbibyte,
}

impl Units {
    fn size_bytes(&self) -> u64 {
        use Units::*;

        match self {
            Byte => 1,
            Sector => 512,
            // binary multiples
            Kibibyte => 1024,
            Mebibyte => 1048576,
            Gibibyte => 1073741824,
            Tebibyte => 1099511627776,
            Pebibyte => 1125899906842624,
            Exbibyte => 1152921504606846976,
            // decimal multiples
            Kilobyte => 1000,
            Megabyte => 1000000,
            Gigabyte => 1000000000,
            Terabyte => 1000000000000,
            Petabyte => 1000000000000000,
            Exabyte => 1000000000000000000,
        }
    }

    pub fn to_string_short(&self) -> String {
        use Units::*;

        String::from(match self {
            Byte => "b",
            Sector => "s",
            // with IEC binary prefixes
            Kibibyte => "KiB",
            Mebibyte => "MiB",
            Gibibyte => "GiB",
            Tebibyte => "Tib",
            Pebibyte => "PiB",
            Exbibyte => "EiB",
            // with SI decimal prefixes
            Kilobyte => "kB", // SI uses lower case 'k' to denote kilo
            Megabyte => "MB",
            Gigabyte => "GB",
            Terabyte => "TB",
            Petabyte => "PB",
            Exabyte => "EB",
        })
    }

    pub fn to_letter(&self) -> String {
        use Units::*;

        // In order to maintain backward compatibilities, we use uppercase letters
        // to denote decimal prefixes.
        String::from(match self {
            Byte => "b",
            Sector => "s",
            // letters for binary prefixes
            Kibibyte => "k",
            Mebibyte => "m",
            Gibibyte => "g",
            Tebibyte => "t",
            Pebibyte => "p",
            Exbibyte => "e",
            // letters for decimal prefixes
            Kilobyte => "K",
            Megabyte => "M",
            Gigabyte => "G",
            Terabyte => "T",
            Petabyte => "P",
            Exabyte => "E",
        })
    }
}

impl FromStr for Units {
    type Err = anyhow::Error;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "byte" | "b" => Ok(Units::Byte),
            "sector" | "s" => Ok(Units::Sector),
            // base 2
            "kibibyte" | "KiB" | "k" => Ok(Units::Kibibyte),
            "mebibyte" | "MiB" | "m" => Ok(Units::Mebibyte),
            "gibibyte" | "GiB" | "g" => Ok(Units::Gibibyte),
            "tebibyte" | "TiB" | "t" => Ok(Units::Tebibyte),
            "pebibyte" | "PiB" | "p" => Ok(Units::Pebibyte),
            "exbibyte" | "EiB" | "e" => Ok(Units::Exbibyte),
            // base 10
            "kilobyte" | "kB" | "K" => Ok(Units::Kilobyte),
            "megabyte" | "MB" | "M" => Ok(Units::Megabyte),
            "gigabyte" | "GB" | "G" => Ok(Units::Gigabyte),
            "terabyte" | "TB" | "T" => Ok(Units::Terabyte),
            "petabyte" | "PB" | "P" => Ok(Units::Petabyte),
            "exabyte" | "EB" | "E" => Ok(Units::Exabyte),
            _ => Err(anyhow!("Invalid unit specifier")),
        }
    }
}

impl ToString for Units {
    fn to_string(&self) -> String {
        use Units::*;

        String::from(match self {
            Byte => "byte",
            Sector => "sector",
            // base 2
            Kibibyte => "kibibyte",
            Mebibyte => "mebibyte",
            Gibibyte => "gibibyte",
            Tebibyte => "terabyte",
            Pebibyte => "pebibyte",
            Exbibyte => "exbibyte",
            // base 10
            Kilobyte => "kilobyte",
            Megabyte => "megabyte",
            Gigabyte => "gigabyte",
            Terabyte => "terabyte",
            Petabyte => "petabyte",
            Exabyte => "exabyte",
        })
    }
}

pub fn to_units(bytes: u64, unit: Units) -> f64 {
    bytes as f64 / unit.size_bytes() as f64
}

//------------------------------------------

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct StorageSize {
    multiple: u64,
    unit: Units,
}

impl StorageSize {
    pub fn new(multiple: u64, unit: Units) -> anyhow::Result<Self> {
        // use division since the unit might not be power of 2
        let limit = u64::MAX / unit.size_bytes();
        if multiple > limit {
            return Err(anyhow!("value out of bounds"));
        }

        Ok(StorageSize { multiple, unit })
    }

    pub fn bytes(multiple: u64) -> anyhow::Result<Self> {
        StorageSize::new(multiple, Units::Byte)
    }

    pub fn sectors(multiple: u64) -> anyhow::Result<Self> {
        StorageSize::new(multiple, Units::Sector)
    }

    pub fn kib(multiple: u64) -> anyhow::Result<Self> {
        StorageSize::new(multiple, Units::Kibibyte)
    }

    pub fn mib(multiple: u64) -> anyhow::Result<Self> {
        StorageSize::new(multiple, Units::Mebibyte)
    }

    pub fn gib(multiple: u64) -> anyhow::Result<Self> {
        StorageSize::new(multiple, Units::Gibibyte)
    }

    pub fn tib(multiple: u64) -> anyhow::Result<Self> {
        StorageSize::new(multiple, Units::Tebibyte)
    }

    pub fn pib(multiple: u64) -> anyhow::Result<Self> {
        StorageSize::new(multiple, Units::Pebibyte)
    }

    pub fn eib(multiple: u64) -> anyhow::Result<Self> {
        StorageSize::new(multiple, Units::Exbibyte)
    }

    pub fn size_bytes(&self) -> u64 {
        self.multiple * self.unit.size_bytes()
    }
}

impl FromStr for StorageSize {
    type Err = anyhow::Error;

    // default to sectors
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let (multiple, unit) = if let Some(pos) = s.find(|c: char| !c.is_ascii_digit()) {
            (s[..pos].parse::<u64>()?, s[pos..].parse::<Units>()?)
        } else {
            (s.parse::<u64>()?, Units::Sector)
        };

        StorageSize::new(multiple, unit)
    }
}

impl ToString for StorageSize {
    fn to_string(&self) -> String {
        let mut s = self.multiple.to_string();
        s.push_str(&self.unit.to_string_short());
        s
    }
}

// Choose the unit that fits the input value, so that the multiple does not exceed 8192.
// The resulting size is a rounded value and might exceed u64::MAX bytes,
// thus the return type is a plain value rather than the StorageSize struct.
// e.g., 1023.5 KiB will be rounded to 1024 KiB, and 15.9 EiB will be rounded to 16 EiB.
// 8192.5 KiB should be converted to 8 MiB, not 8193 KiB, since the rounded value exceeds
// the 8192 threshold.
pub fn to_pretty_print_size(bytes: u64) -> (u64, Units) {
    use Units::*;
    const UNITS: [Units; 7] = [
        Byte, Kibibyte, Mebibyte, Gibibyte, Tebibyte, Pebibyte, Exbibyte,
    ];

    // Choose the ith unit so that the initial multiple falls in the range [1024, 1048575] if
    // possible.
    let mut i = if bytes > 0 {
        // (63 - leading_zeros) is the index of the highest non-zero bit
        ((63 - bytes.leading_zeros()) / 10).saturating_sub(1)
    } else {
        0
    };

    let mut multiple = bytes >> (10 * i);
    if multiple > 8192 {
        multiple = (multiple as f64 / 1024.0).round() as u64;
        i += 1;
    }

    (multiple, UNITS[i as usize])
}

//------------------------------------------

#[cfg(test)]
mod storage_size_tests {
    use super::*;

    #[test]
    fn test_from_string() {
        // default to sectors
        assert_eq!(
            StorageSize::from_str("1024").unwrap(),
            StorageSize::sectors(1024).unwrap()
        );
        // bytes
        assert_eq!(
            StorageSize::from_str("1024b").unwrap(),
            StorageSize::bytes(1024).unwrap()
        );
        // KiB
        assert_eq!(
            StorageSize::from_str("1024k").unwrap(),
            StorageSize::kib(1024).unwrap()
        );
        // MiB
        assert_eq!(
            StorageSize::from_str("1024m").unwrap(),
            StorageSize::mib(1024).unwrap()
        );
        // GiB
        assert_eq!(
            StorageSize::from_str("1024g").unwrap(),
            StorageSize::gib(1024).unwrap()
        );
        // TiB
        assert_eq!(
            StorageSize::from_str("1024t").unwrap(),
            StorageSize::tib(1024).unwrap()
        );
        // PiB
        assert_eq!(
            StorageSize::from_str("1024p").unwrap(),
            StorageSize::pib(1024).unwrap()
        );
        // EiB
        assert_eq!(
            StorageSize::from_str("15e").unwrap(),
            StorageSize::eib(15).unwrap()
        );
    }

    #[test]
    fn test_out_of_bounds() {
        assert!(StorageSize::from_str("18446744073709551616b").is_err()); // bytes
        assert!(StorageSize::from_str("36028797018963968").is_err()); // sectors
        assert!(StorageSize::from_str("18014398509481984k").is_err()); // KiB
        assert!(StorageSize::from_str("17592186044416m").is_err()); // MiB
        assert!(StorageSize::from_str("17179869184g").is_err()); // GiB
        assert!(StorageSize::from_str("16777216t").is_err()); // TiB
        assert!(StorageSize::from_str("16384p").is_err()); // PiB
        assert!(StorageSize::from_str("16e").is_err()); // EiB
    }

    #[test]
    fn test_numeric_conversion() {
        let orig = StorageSize::from_str("18446744073709551615b").unwrap();
        let converted = StorageSize::bytes(orig.size_bytes()).unwrap();
        assert_eq!(orig, converted);
    }

    #[test]
    fn test_string_conversion() {
        let orig = StorageSize::from_str("18446744073709551615b").unwrap();
        let converted = StorageSize::from_str(&orig.to_string()).unwrap();
        assert_eq!(orig, converted);
    }

    #[test]
    fn test_pretty_print_max_multiples() {
        assert_eq!(
            to_pretty_print_size(8 * u64::pow(2, 10)),
            (8192, Units::Byte)
        );
        assert_eq!(
            to_pretty_print_size(8 * u64::pow(2, 20)),
            (8192, Units::Kibibyte)
        );
        assert_eq!(
            to_pretty_print_size(8 * u64::pow(2, 30)),
            (8192, Units::Mebibyte)
        );
        assert_eq!(
            to_pretty_print_size(8 * u64::pow(2, 40)),
            (8192, Units::Gibibyte)
        );
        assert_eq!(
            to_pretty_print_size(8 * u64::pow(2, 50)),
            (8192, Units::Tebibyte)
        );
        assert_eq!(
            to_pretty_print_size(8 * u64::pow(2, 60)),
            (8192, Units::Pebibyte)
        );
    }

    #[test]
    fn test_pretty_print_min_multiples() {
        assert_eq!(to_pretty_print_size(1), (1, Units::Byte));
        // round down 8193 bytes to 8 KiB
        assert_eq!(
            to_pretty_print_size(8 * u64::pow(2, 10) + 1),
            (8, Units::Kibibyte)
        );
        // round down 8192.001 KiB to 8 MiB
        assert_eq!(
            to_pretty_print_size(8 * u64::pow(2, 20) + u64::pow(2, 10)),
            (8, Units::Mebibyte)
        );
        // round down 8192.001 MiB to 8 GiB
        assert_eq!(
            to_pretty_print_size(8 * u64::pow(2, 30) + u64::pow(2, 20)),
            (8, Units::Gibibyte)
        );
        // round down 8192.001 GiB to 8 TiB
        assert_eq!(
            to_pretty_print_size(8 * u64::pow(2, 40) + u64::pow(2, 30)),
            (8, Units::Tebibyte)
        );
        // round down 8192.001 TiB to 8 PiB
        assert_eq!(
            to_pretty_print_size(8 * u64::pow(2, 50) + u64::pow(2, 40)),
            (8, Units::Pebibyte)
        );
        // round down 8192.001 PiB to 8 EiB
        assert_eq!(
            to_pretty_print_size(8 * u64::pow(2, 60) + u64::pow(2, 50)),
            (8, Units::Exbibyte)
        );
    }

    #[test]
    fn test_pretty_print_min_max_bytes() {
        assert_eq!(to_pretty_print_size(0), (0, Units::Byte));
        assert_eq!(to_pretty_print_size(u64::MAX), (16, Units::Exbibyte));
    }

    #[test]
    fn test_pretty_print_round_up() {
        // round up 8.5 KiB to 9 KiB
        assert_eq!(to_pretty_print_size(8 * 1024 + 512), (9, Units::Kibibyte));

        // round up 1023.5 KiB to 1024 KiB
        assert_eq!(
            to_pretty_print_size(1023 * 1024 + 512),
            (1024, Units::Kibibyte)
        );
    }
}

//------------------------------------------
