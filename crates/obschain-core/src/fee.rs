/// Safe normalized fee-rate calculation.
///
/// Bitcoin transaction fee rate is standardly measured in satoshis per virtual byte (sat/vB).
///
/// Edge case handling:
/// - Returns `None` if `vsize == 0`.
/// - Returns `None` if `fee_sats` is missing.
/// - Does not panic on integer overflow or invalid inputs.
pub fn calculate_fee_rate_sat_vb(fee_sats: u64, vsize: u64) -> Option<f64> {
    if vsize == 0 {
        return None;
    }
    // Safe conversion to f64 for rate calculation
    let rate = fee_sats as f64 / vsize as f64;
    if rate.is_nan() || rate.is_infinite() || rate < 0.0 {
        None
    } else {
        Some(rate)
    }
}

/// Derives virtual size (vsize) from transaction weight according to BIP-141:
/// `vsize = ceil(weight / 4)`
///
/// Uses integer ceiling division without floating point inaccuracies.
pub fn calculate_vsize_from_weight(weight: u64) -> u64 {
    weight.div_ceil(4)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_fee_rate_normal() {
        let rate = calculate_fee_rate_sat_vb(1000, 200);
        assert_eq!(rate, Some(5.0));

        let rate_frac = calculate_fee_rate_sat_vb(350, 140);
        assert_eq!(rate_frac, Some(2.5));
    }

    #[test]
    fn test_zero_vsize_returns_none() {
        let rate = calculate_fee_rate_sat_vb(5000, 0);
        assert!(rate.is_none());
    }

    #[test]
    fn test_vsize_from_weight() {
        assert_eq!(calculate_vsize_from_weight(0), 0);
        assert_eq!(calculate_vsize_from_weight(4), 1);
        assert_eq!(calculate_vsize_from_weight(5), 2);
        assert_eq!(calculate_vsize_from_weight(8), 2);
        assert_eq!(calculate_vsize_from_weight(9), 3);
        assert_eq!(calculate_vsize_from_weight(400), 100);
    }
}
