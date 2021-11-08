use anyhow::anyhow;
use std::str::FromStr;

//------------------------------------------

#[derive(Copy, Clone)]
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
            // base 2
            Kibibyte => 1024,
            Mebibyte => 1048576,
            Gibibyte => 1073741824,
            Tebibyte => 1099511627776,
            Pebibyte => 1125899906842624,
            Exbibyte => 1152921504606846976,
            // base 10
            Kilobyte => 1000,
            Megabyte => 1000000,
            Gigabyte => 1000000000,
            Terabyte => 1000000000000,
            Petabyte => 1000000000000000,
            Exabyte => 1000000000000000000,
        }
    }
}

impl FromStr for Units {
    type Err = anyhow::Error;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "byte" | "b" => Ok(Units::Byte),
            "sector" | "s" => Ok(Units::Sector),
            // base 2
            "kibibyte" | "k" => Ok(Units::Kibibyte),
            "mibibyte" | "m" => Ok(Units::Mebibyte),
            "gibibyte" | "g" => Ok(Units::Gibibyte),
            "tebibyte" | "t" => Ok(Units::Tebibyte),
            "pebibyte" | "p" => Ok(Units::Pebibyte),
            "exbibyte" | "e" => Ok(Units::Exbibyte),
            // base 10
            "kilobyte" | "K" => Ok(Units::Kilobyte),
            "megabyte" | "M" => Ok(Units::Megabyte),
            "gigabyte" | "G" => Ok(Units::Gigabyte),
            "terabyte" | "T" => Ok(Units::Terabyte),
            "petabyte" | "P" => Ok(Units::Petabyte),
            "exabyte" | "E" => Ok(Units::Exabyte),
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
            Mebibyte => "mibibyte",
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

pub fn to_bytes(size: u64, unit: Units) -> u64 {
    size * unit.size_bytes()
}

pub fn to_units(bytes: u64, unit: Units) -> f64 {
    bytes as f64 / unit.size_bytes() as f64
}

//------------------------------------------
