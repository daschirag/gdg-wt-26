use serde::{Deserialize, Serialize};

#[derive(Debug, Serialize, Deserialize)]
pub struct DeltaEncoded<T> {
    pub base: T,
    pub deltas: Vec<i64>, // Deltas can be negative or larger than T if not careful, use i64 for safety
}

pub struct DeltaEncoder;

impl DeltaEncoder {
    pub fn encode_i64(values: &[i64]) -> DeltaEncoded<i64> {
        if values.is_empty() {
            return DeltaEncoded {
                base: 0,
                deltas: Vec::new(),
            };
        }

        let base = values[0];
        let mut deltas = Vec::with_capacity(values.len());
        // Note: we store deltas as absolute or relative from previous
        // Standard Delta-of-Delta or simple Delta. Simple Delta for now.

        let mut prev = base;
        for &val in values.iter().skip(1) {
            deltas.push(val - prev);
            prev = val;
        }

        DeltaEncoded { base, deltas }
    }

    pub fn decode_i64(encoded: &DeltaEncoded<i64>) -> crate::utils::aligned_vec::AlignedVec<i64> {
        let mut values =
            crate::utils::aligned_vec::AlignedVec::with_capacity(encoded.deltas.len() + 1);
        let mut current = encoded.base;
        values.push(current);

        for &delta in &encoded.deltas {
            current += delta;
            values.push(current);
        }

        values
    }

    // SIMD-friendly vectorized addition if needed,
    // but the loop above is usually auto-vectorized by LLVM.
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_delta_roundtrip() {
        let data = vec![100i64, 105, 110, 108, 120];
        let encoded = DeltaEncoder::encode_i64(&data);
        let decoded = DeltaEncoder::decode_i64(&encoded);
        assert_eq!(data, decoded);
    }
}
