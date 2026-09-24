use serde::{Deserialize, Serialize};
use thiserror::Error;

pub const SATS_PER_BTC: u64 = 100_000_000;
pub const MAX_MONEY_SATS: u64 = 21_000_000 * SATS_PER_BTC;

#[derive(Debug, Error, PartialEq, Eq, Clone, Serialize, Deserialize)]
pub enum AmountError {
    #[error("Negative Bitcoin amounts are invalid")]
    Negative,

    #[error("Non-finite float value (NaN or infinity)")]
    NonFinite,

    #[error("Amount exceeds maximum possible Bitcoin supply (21M BTC)")]
    ExceedsMaxSupply,

    #[error("Invalid decimal representation: {0}")]
    InvalidDecimal(String),

    #[error("Overflow during satoshi calculation")]
    Overflow,
}

/// Safely converts a decimal BTC string (e.g. "1.5", "0.00000001", "21000000")
/// into integer satoshis using exact integer arithmetic without float rounding errors.
pub fn btc_str_to_sats(btc_str: &str) -> Result<u64, AmountError> {
    let s = btc_str.trim();
    if s.is_empty() {
        return Err(AmountError::InvalidDecimal("empty string".to_string()));
    }
    if s.starts_with('-') {
        return Err(AmountError::Negative);
    }

    let mut parts = s.split('.');
    let whole_str = parts.next().unwrap_or("0");
    let whole_str = if whole_str.is_empty() { "0" } else { whole_str };
    let frac_str = parts.next().unwrap_or("");

    if parts.next().is_some() {
        return Err(AmountError::InvalidDecimal(
            "multiple decimal points".to_string(),
        ));
    }

    let whole: u64 = whole_str
        .parse()
        .map_err(|_| AmountError::InvalidDecimal("invalid whole part".to_string()))?;

    // Satoshis have 8 decimal places
    let mut frac_padded = [b'0'; 8];
    let frac_bytes = frac_str.as_bytes();
    for i in 0..8 {
        if i < frac_bytes.len() {
            if !frac_bytes[i].is_ascii_digit() {
                return Err(AmountError::InvalidDecimal(
                    "non-digit in fraction".to_string(),
                ));
            }
            frac_padded[i] = frac_bytes[i];
        }
    }

    let frac_val: u64 = std::str::from_utf8(&frac_padded)
        .map_err(|_| AmountError::InvalidDecimal("utf8 error".to_string()))?
        .parse()
        .map_err(|_| AmountError::InvalidDecimal("invalid frac part".to_string()))?;

    let whole_sats = whole
        .checked_mul(SATS_PER_BTC)
        .ok_or(AmountError::Overflow)?;

    let total = whole_sats
        .checked_add(frac_val)
        .ok_or(AmountError::Overflow)?;

    if total > MAX_MONEY_SATS {
        return Err(AmountError::ExceedsMaxSupply);
    }

    Ok(total)
}

/// Fallback for external APIs that expose float directly (e.g. mempoolInfo total_fee).
/// Performs strict boundary, non-finite, and overflow validation.
pub fn safe_btc_f64_to_sats(btc: f64) -> Result<u64, AmountError> {
    if !btc.is_finite() {
        return Err(AmountError::NonFinite);
    }
    if btc < 0.0 {
        return Err(AmountError::Negative);
    }
    if btc > 21_000_000.0 {
        return Err(AmountError::ExceedsMaxSupply);
    }

    // Format float with up to 8 decimal places and parse with exact decimal integer logic
    let formatted = format!("{btc:.8}");
    btc_str_to_sats(&formatted)
}

/// Calculates coin age destroyed in satoshi-days without integer overflow using u128.
pub fn calculate_satoshi_days(value_sats: u64, age_days: u64) -> u128 {
    (value_sats as u128) * (age_days as u128)
}

/// Formats satoshi-days to BTC-days for presentation / API response.
pub fn satoshi_days_to_btc_days(satoshi_days: u128) -> f64 {
    (satoshi_days as f64) / (SATS_PER_BTC as f64)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_btc_str_to_sats_exact() {
        assert_eq!(btc_str_to_sats("1").unwrap(), 100_000_000);
        assert_eq!(btc_str_to_sats("1.0").unwrap(), 100_000_000);
        assert_eq!(btc_str_to_sats("0.00000001").unwrap(), 1);
        assert_eq!(btc_str_to_sats("0.5").unwrap(), 50_000_000);
        assert_eq!(btc_str_to_sats(".5").unwrap(), 50_000_000);
        assert_eq!(btc_str_to_sats("21000000").unwrap(), MAX_MONEY_SATS);
    }

    #[test]
    fn test_btc_str_to_sats_errors() {
        assert_eq!(btc_str_to_sats("-1.0"), Err(AmountError::Negative));
        assert_eq!(
            btc_str_to_sats("21000001.0"),
            Err(AmountError::ExceedsMaxSupply)
        );
        assert!(matches!(
            btc_str_to_sats("1.2.3"),
            Err(AmountError::InvalidDecimal(_))
        ));
        assert!(matches!(
            btc_str_to_sats("abc"),
            Err(AmountError::InvalidDecimal(_))
        ));
    }

    #[test]
    fn test_safe_btc_f64_to_sats() {
        assert_eq!(safe_btc_f64_to_sats(3.996).unwrap(), 399_600_000);
        assert_eq!(safe_btc_f64_to_sats(0.0).unwrap(), 0);
        assert_eq!(safe_btc_f64_to_sats(f64::NAN), Err(AmountError::NonFinite));
        assert_eq!(
            safe_btc_f64_to_sats(f64::INFINITY),
            Err(AmountError::NonFinite)
        );
        assert_eq!(safe_btc_f64_to_sats(-0.01), Err(AmountError::Negative));
    }

    #[test]
    fn test_satoshi_days_calculation_no_overflow() {
        // 21,000,000 BTC held for 15 years (~5475 days)
        let sats = MAX_MONEY_SATS;
        let days = 5475u64;
        let sat_days = calculate_satoshi_days(sats, days);
        assert_eq!(sat_days, (MAX_MONEY_SATS as u128) * 5475);
        let btc_days = satoshi_days_to_btc_days(sat_days);
        assert!((btc_days - (21_000_000.0 * 5475.0)).abs() < 1e-4);
    }
}
