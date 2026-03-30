use serde::{Deserialize, Serialize};

#[derive(Debug, Serialize, Deserialize)]
pub struct RleRun {
    pub value: u8,
    pub length: u32,
}

pub struct RleEncoder;

impl RleEncoder {
    pub fn encode(values: &[u8]) -> Vec<RleRun> {
        if values.is_empty() {
            return Vec::new();
        }

        let mut runs = Vec::new();
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

        // Push last run
        runs.push(RleRun {
            value: current_val,
            length: current_len,
        });

        runs
    }

    pub fn decode(runs: &[RleRun]) -> Vec<u8> {
        let total_size: usize = runs.iter().map(|r| r.length as usize).sum();
        let mut values = Vec::with_capacity(total_size);
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
    fn test_rle_roundtrip() {
        let data = vec![0, 0, 0, 1, 1, 2, 2, 2, 2, 0];
        let encoded = RleEncoder::encode(&data);
        assert_eq!(encoded.len(), 4);
        assert_eq!(encoded[0].value, 0);
        assert_eq!(encoded[0].length, 3);
        
        let decoded = RleEncoder::decode(&encoded);
        assert_eq!(data, decoded);
    }
}
