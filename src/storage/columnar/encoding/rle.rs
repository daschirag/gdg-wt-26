use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct RleRun<T> {
    pub value: T,
    pub length: u32,
}

pub struct RleEncoder;

impl RleEncoder {
    pub fn encode<T: PartialEq + Copy>(values: &[T]) -> Vec<RleRun<T>> {
        if values.is_empty() {
            return Vec::new();
        }

        let mut runs = Vec::with_capacity(values.len() / 4); // Heuristic initial capacity
        let mut current_val = values[0];
        let mut current_len = 1;

        for &val in values.iter().skip(1) {
            if val == current_val {
                current_len += 1;
            } else {
                runs.push(RleRun {
                    value: current_val,
                    length: current_len,
                });
                current_val = val;
                current_len = 1;
            }
        }

        runs.push(RleRun {
            value: current_val,
            length: current_len,
        });

        runs
    }

    pub fn decode<T: Copy>(runs: &[RleRun<T>]) -> crate::utils::aligned_vec::AlignedVec<T> {
        let total_size: usize = runs.iter().map(|r| r.length as usize).sum();
        let mut values = crate::utils::aligned_vec::AlignedVec::with_capacity(total_size);
        for run in runs {
            for _ in 0..run.length {
                values.push(run.value);
            }
        }
        values
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_rle_u8() {
        let data = vec![0u8, 0, 0, 1, 1, 2, 2, 2, 2, 0];
        let encoded = RleEncoder::encode(&data);
        assert_eq!(encoded.len(), 4);
        let decoded = RleEncoder::decode(&encoded);
        assert_eq!(data, decoded);
    }

    #[test]
    fn test_rle_i64() {
        let data = vec![100i64, 100, 100, 200, 200];
        let encoded = RleEncoder::encode(&data);
        assert_eq!(encoded.len(), 2);
        let decoded = RleEncoder::decode(&encoded);
        assert_eq!(data, decoded);
    }
}
