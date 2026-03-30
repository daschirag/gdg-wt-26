use serde::{Deserialize, Serialize};

#[derive(Debug, Serialize, Deserialize)]
pub struct DeltaEncoded {
    pub base: u64,
    pub deltas: Vec<i32>,
}

pub struct DeltaEncoder;

impl DeltaEncoder {
    pub fn encode(values: &[u64]) -> DeltaEncoded {
        if values.is_empty() {
            return DeltaEncoded { base: 0, deltas: Vec::new() };
        }

        let base = values[0];
        let mut deltas = Vec::with_capacity(values.len() - 1);
        
        for i in 1..values.len() {
            let delta = (values[i] as i64 - values[i-1] as i64) as i32;
            deltas.push(delta);
        }

        DeltaEncoded { base, deltas }
    }

    pub fn decode(encoded: &DeltaEncoded) -> Vec<u64> {
        if encoded.deltas.is_empty() && encoded.base == 0 {
            // Technically could be a single 0, but we handle empty properly
            // if we need to. For now, we assume if base is set, there's at least one value.
        }
        
        let mut values = Vec::with_capacity(encoded.deltas.len() + 1);
        let mut current = encoded.base;
        values.push(current);

        for &delta in &encoded.deltas {
            current = (current as i64 + delta as i64) as u64;
            values.push(current);
        }

        values
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_delta_roundtrip() {
        let data = vec![100, 105, 110, 108, 120];
        let encoded = DeltaEncoder::encode(&data);
        assert_eq!(encoded.base, 100);
        assert_eq!(encoded.deltas, vec![5, 5, -2, 12]);
        
        let decoded = DeltaEncoder::decode(&encoded);
        assert_eq!(data, decoded);
    }

    #[test]
    fn test_monotonic_delta() {
        let data = vec![1000, 1001, 1002, 1003];
        let encoded = DeltaEncoder::encode(&data);
        assert_eq!(encoded.deltas, vec![1, 1, 1]);
    }
}
