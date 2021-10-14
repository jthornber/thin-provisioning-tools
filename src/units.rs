use anyhow::anyhow;
use std::str::FromStr;

//------------------------------------------

#[derive(Clone)]
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
        match self {
            Units::Byte => 1,
            Units::Sector => 512,
            // base 2
            Units::Kibibyte => 1024,
            Units::Mebibyte => 1048576,
            Units::Gibibyte => 1073741824,
            Units::Tebibyte => 1099511627776,
            Units::Pebibyte => 1125899906842624,
            Units::Exbibyte => 1152921504606846976,
            // base 10
            Units::Kilobyte => 1000,
            Units::Megabyte => 1000000,
            Units::Gigabyte => 1000000000,
            Units::Terabyte => 1000000000000,
            Units::Petabyte => 1000000000000000,
            Units::Exabyte => 1000000000000000000,
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
        String::from(match self {
            Units::Byte => "byte",
            Units::Sector => "sector",
            // base 2
            Units::Kibibyte => "kibibyte",
            Units::Mebibyte => "mibibyte",
            Units::Gibibyte => "gibibyte",
            Units::Tebibyte => "terabyte",
            Units::Pebibyte => "pebibyte",
            Units::Exbibyte => "exbibyte",
            // base 10
            Units::Kilobyte => "kilobyte",
            Units::Megabyte => "megabyte",
            Units::Gigabyte => "gigabyte",
            Units::Terabyte => "terabyte",
            Units::Petabyte => "petabyte",
            Units::Exabyte => "exabyte",
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
