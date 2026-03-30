use bloomfilter::Bloom;
use serde::{Deserialize, Serialize};

#[derive(Clone)]
pub struct BloomFilterWrapper {
    pub fpr: f64,
    pub bit_vec_len: u32,
    pub k: u32,
    pub sip_keys: [(u64, u64); 2],
    pub inner: Bloom<u64>,
}

impl Serialize for BloomFilterWrapper {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::SerializeStruct;
        let mut state = serializer.serialize_struct("BloomFilterWrapper", 5)?;
        state.serialize_field("fpr", &self.fpr)?;
        state.serialize_field("bit_vec_len", &self.bit_vec_len)?;
        state.serialize_field("k", &self.k)?;
        state.serialize_field("sip_keys", &self.sip_keys)?;
        state.serialize_field("inner", &self.inner.bitmap())?;
        state.end()
    }
}

impl<'de> Deserialize<'de> for BloomFilterWrapper {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        #[derive(Deserialize)]
        struct BloomData {
            fpr: f64,
            bit_vec_len: u32,
            k: u32,
            sip_keys: [(u64, u64); 2],
            inner: Vec<u8>,
        }
        let data = BloomData::deserialize(deserializer)?;
        let bloom =
            Bloom::from_existing(&data.inner, data.bit_vec_len as u64, data.k, data.sip_keys);

        Ok(Self {
            fpr: data.fpr,
            bit_vec_len: data.bit_vec_len,
            k: data.k,
            sip_keys: data.sip_keys,
            inner: bloom,
        })
    }
}

impl BloomFilterWrapper {
    pub fn new(num_items: usize, fpr: f64) -> Self {
        let bloom = Bloom::new_for_fp_rate(num_items, fpr);
        Self {
            fpr,
            bit_vec_len: bloom.number_of_bits() as u32,
            k: bloom.number_of_hash_functions(),
            sip_keys: bloom.sip_keys(),
            inner: bloom,
        }
    }

    pub fn insert(&mut self, key: u64) {
        self.inner.set(&key);
    }

    pub fn contains(&self, key: u64) -> bool {
        self.inner.check(&key)
    }
}
