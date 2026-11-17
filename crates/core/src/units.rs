//! Human-readable sizes (`"2GiB"`) and durations (`"1m30s"`) for config files.

use std::fmt;
use std::str::FromStr;
use std::time::Duration;

use serde::{Deserialize, Serialize};

/// A size in bytes, written as `"512MiB"`, `"2GiB"`, `"1.5GB"` or a plain byte count.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(try_from = "SizeRepr", into = "String")]
pub struct ByteSize(pub u64);

impl ByteSize {
    /// `n` mebibytes.
    pub const fn mib(n: u64) -> Self {
        ByteSize(n * 1024 * 1024)
    }
    /// `n` gibibytes.
    pub const fn gib(n: u64) -> Self {
        ByteSize(n * 1024 * 1024 * 1024)
    }
    /// The byte count.
    pub fn bytes(self) -> u64 {
        self.0
    }
}

/// Error from parsing a [`ByteSize`] or [`HumanDuration`].
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
#[error("cannot parse {input:?}: {reason}")]
pub struct UnitError {
    /// The text that failed.
    pub input: String,
    /// Why.
    pub reason: &'static str,
}

fn unit_err(input: &str, reason: &'static str) -> UnitError {
    UnitError {
        input: input.to_owned(),
        reason,
    }
}

/// Splits `"1.5GiB"` into `(1.5, "GiB")`.
fn split_number(s: &str) -> Option<(f64, &str)> {
    let end = s
        .find(|c: char| !(c.is_ascii_digit() || c == '.'))
        .unwrap_or(s.len());
    if end == 0 {
        return None;
    }
    let n: f64 = s[..end].parse().ok()?;
    Some((n, s[end..].trim_start()))
}

impl FromStr for ByteSize {
    type Err = UnitError;
    fn from_str(input: &str) -> Result<Self, UnitError> {
        let s = input.trim();
        let (n, unit) = split_number(s).ok_or_else(|| unit_err(input, "expected a number"))?;
        let mult: u64 = match unit.to_ascii_lowercase().as_str() {
            "" | "b" => 1,
            "kb" => 1_000,
            "kib" | "k" => 1 << 10,
            "mb" => 1_000_000,
            "mib" | "m" => 1 << 20,
            "gb" => 1_000_000_000,
            "gib" | "g" => 1 << 30,
            "tb" => 1_000_000_000_000,
            "tib" | "t" => 1 << 40,
            _ => {
                return Err(unit_err(
                    input,
                    "unknown unit; use B, KiB, MiB, GiB, TiB or KB, MB, GB, TB",
                ))
            }
        };
        let bytes = n * mult as f64;
        if !bytes.is_finite() || bytes > u64::MAX as f64 {
            return Err(unit_err(input, "too large"));
        }
        Ok(ByteSize(bytes as u64))
    }
}

impl fmt::Display for ByteSize {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        const UNITS: [(u64, &str); 4] = [
            (1 << 40, "TiB"),
            (1 << 30, "GiB"),
            (1 << 20, "MiB"),
            (1 << 10, "KiB"),
        ];
        for (size, name) in UNITS {
            if self.0 >= size && self.0.is_multiple_of(size) {
                return write!(f, "{}{}", self.0 / size, name);
            }
        }
        write!(f, "{}B", self.0)
    }
}

impl From<ByteSize> for String {
    fn from(b: ByteSize) -> String {
        b.to_string()
    }
}

/// Config files may write sizes as strings or bare numbers.
#[derive(Deserialize)]
#[serde(untagged)]
enum SizeRepr {
    Num(u64),
    Text(String),
}

impl TryFrom<SizeRepr> for ByteSize {
    type Error = UnitError;
    fn try_from(r: SizeRepr) -> Result<Self, UnitError> {
        match r {
            SizeRepr::Num(n) => Ok(ByteSize(n)),
            SizeRepr::Text(s) => s.parse(),
        }
    }
}

/// A duration written as `"150ms"`, `"30s"`, `"5m"`, `"1h"`, `"1d"` or a sum like `"1m30s"`.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(try_from = "String", into = "String")]
pub struct HumanDuration(pub Duration);

impl HumanDuration {
    /// `n` milliseconds.
    pub const fn millis(n: u64) -> Self {
        HumanDuration(Duration::from_millis(n))
    }
    /// `n` seconds.
    pub const fn secs(n: u64) -> Self {
        HumanDuration(Duration::from_secs(n))
    }
    /// The duration.
    pub fn get(self) -> Duration {
        self.0
    }
}

impl FromStr for HumanDuration {
    type Err = UnitError;
    fn from_str(input: &str) -> Result<Self, UnitError> {
        let mut rest = input.trim();
        if rest.is_empty() {
            return Err(unit_err(input, "empty"));
        }
        let mut total = Duration::ZERO;
        while !rest.is_empty() {
            let (n, after) =
                split_number(rest).ok_or_else(|| unit_err(input, "expected a number"))?;
            let unit_len = after
                .find(|c: char| !c.is_ascii_alphabetic())
                .unwrap_or(after.len());
            let unit_secs: f64 = match &after[..unit_len] {
                "ms" => 0.001,
                "s" => 1.0,
                "m" => 60.0,
                "h" => 3_600.0,
                "d" => 86_400.0,
                "" => return Err(unit_err(input, "missing unit; use ms, s, m, h or d")),
                _ => return Err(unit_err(input, "unknown unit; use ms, s, m, h or d")),
            };
            let part = Duration::try_from_secs_f64(n * unit_secs)
                .map_err(|_| unit_err(input, "out of range"))?;
            total = total
                .checked_add(part)
                .ok_or_else(|| unit_err(input, "out of range"))?;
            rest = after[unit_len..].trim_start();
        }
        Ok(HumanDuration(total))
    }
}

impl TryFrom<String> for HumanDuration {
    type Error = UnitError;
    fn try_from(s: String) -> Result<Self, UnitError> {
        s.parse()
    }
}

impl fmt::Display for HumanDuration {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let ms = self.0.as_millis();
        if ms.is_multiple_of(3_600_000) && ms > 0 {
            write!(f, "{}h", ms / 3_600_000)
        } else if ms.is_multiple_of(60_000) && ms > 0 {
            write!(f, "{}m", ms / 60_000)
        } else if ms.is_multiple_of(1_000) {
            write!(f, "{}s", ms / 1_000)
        } else {
            write!(f, "{ms}ms")
        }
    }
}

impl From<HumanDuration> for String {
    fn from(d: HumanDuration) -> String {
        d.to_string()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn sizes() {
        let p = |s: &str| s.parse::<ByteSize>().map(ByteSize::bytes);
        assert_eq!(p("2GiB"), Ok(2 << 30));
        assert_eq!(p("256 MiB"), Ok(256 << 20));
        assert_eq!(p("1.5GB"), Ok(1_500_000_000));
        assert_eq!(p("4096"), Ok(4096));
        assert!(p("lots").is_err());
        assert!(p("2 parsecs").is_err());
        assert_eq!(ByteSize::gib(2).to_string(), "2GiB");
    }

    #[test]
    fn durations() {
        let p = |s: &str| s.parse::<HumanDuration>().map(HumanDuration::get);
        assert_eq!(p("150ms"), Ok(Duration::from_millis(150)));
        assert_eq!(p("5m"), Ok(Duration::from_secs(300)));
        assert_eq!(p("1m30s"), Ok(Duration::from_secs(90)));
        assert_eq!(p("1.5s"), Ok(Duration::from_millis(1500)));
        assert!(p("").is_err());
        assert!(p("30").is_err(), "unit required");
        assert!(p("3 weeks").is_err());
        assert_eq!(HumanDuration::secs(300).to_string(), "5m");
        assert_eq!(HumanDuration::millis(150).to_string(), "150ms");
    }
}
