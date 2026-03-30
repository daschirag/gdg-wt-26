use crate::errors::StorageError;
use memmap2::Mmap;
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, HashMap};
use std::fs::{self, File};
use std::path::Path;

pub const BITMAP_MAGIC: [u8; 4] = *b"BMP1";
pub const BITMAP_VERSION: u32 = 1;

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Bitmap {
    len: usize,
    words: Vec<u64>,
}

impl Bitmap {
    pub fn new(len: usize) -> Self {
        Self {
            len,
            words: vec![0; len.div_ceil(64)],
        }
    }

    pub fn full(len: usize) -> Self {
        let mut bitmap = Self::new(len);
        bitmap.set_range(0, len);
        bitmap
    }

    pub fn len(&self) -> usize {
        self.len
    }

    pub fn set(&mut self, idx: usize) {
        if idx >= self.len {
            return;
        }
        let word = idx / 64;
        let bit = idx % 64;
        self.words[word] |= 1u64 << bit;
    }

    /// Set all bits in [start, start+len) using word-level fills where possible.
    pub fn set_range(&mut self, start: usize, len: usize) {
        if len == 0 {
            return;
        }
        let end = (start + len).min(self.len);
        let first_word = start / 64;
        let last_word = (end - 1) / 64;
        if first_word == last_word {
            let lo = start % 64;
            let hi = end % 64;
            let hi = if hi == 0 { 64 } else { hi };
            let mask = if hi - lo == 64 { u64::MAX } else { ((1u64 << (hi - lo)) - 1) << lo };
            self.words[first_word] |= mask;
        } else {
            // First partial word
            let lo = start % 64;
            self.words[first_word] |= u64::MAX << lo;
            // Full middle words
            for w in (first_word + 1)..last_word {
                self.words[w] = u64::MAX;
            }
            // Last partial word
            let hi = end % 64;
            let mask = if hi == 0 { u64::MAX } else { (1u64 << hi) - 1 };
            self.words[last_word] |= mask;
        }
    }

    pub fn contains(&self, idx: usize) -> bool {
        if idx >= self.len {
            return false;
        }
        let word = idx / 64;
        let bit = idx % 64;
        (self.words[word] & (1u64 << bit)) != 0
    }

    pub fn count_ones(&self) -> u64 {
        self.words.iter().map(|w| w.count_ones() as u64).sum()
    }

    pub fn and(&self, other: &Bitmap) -> Bitmap {
        let mut out = Bitmap::new(self.len.min(other.len));
        for i in 0..out.words.len() {
            out.words[i] = self.words[i] & other.words[i];
        }
        out
    }

    pub fn and_inplace(&mut self, other: &Bitmap) {
        let out_len = self.len.min(other.len);
        self.len = out_len;
        self.words.truncate(out_len.div_ceil(64));
        for i in 0..self.words.len() {
            self.words[i] &= other.words[i];
        }
    }

    pub fn and_count(&self, other: &Bitmap) -> u64 {
        let words = self.words.len().min(other.words.len());
        let mut count = 0u64;
        for i in 0..words {
            count += (self.words[i] & other.words[i]).count_ones() as u64;
        }
        count
    }

    pub fn or(&self, other: &Bitmap) -> Bitmap {
        let mut out = Bitmap::new(self.len.min(other.len));
        for i in 0..out.words.len() {
            out.words[i] = self.words[i] | other.words[i];
        }
        out
    }

    pub fn or_inplace(&mut self, other: &Bitmap) {
        let out_len = self.len.min(other.len);
        self.len = out_len;
        self.words.truncate(out_len.div_ceil(64));
        for i in 0..self.words.len() {
            self.words[i] |= other.words[i];
        }
    }

    pub fn not(&self) -> Bitmap {
        let mut out = Bitmap::new(self.len);
        for i in 0..self.words.len() {
            out.words[i] = !self.words[i];
        }
        // Clear padding bits.
        let trailing = self.len % 64;
        if trailing != 0 {
            let mask = (1u64 << trailing) - 1;
            let last = out.words.len() - 1;
            out.words[last] &= mask;
        }
        out
    }

    pub fn iter_ones(&self) -> BitmapOnes<'_> {
        BitmapOnes {
            bitmap: self,
            word_idx: 0,
            current_word: self.words.first().copied().unwrap_or(0),
        }
    }

    /// Count set bits in [start, end) using word-level popcount.
    pub fn count_ones_range(&self, start: usize, end: usize) -> u64 {
        let end = end.min(self.len);
        if start >= end {
            return 0;
        }
        let first_word = start / 64;
        let last_word = (end - 1) / 64;
        if first_word == last_word {
            let lo = start % 64;
            let hi = end % 64;
            let hi = if hi == 0 { 64 } else { hi };
            let mask = if hi - lo == 64 { u64::MAX } else { ((1u64 << (hi - lo)) - 1) << lo };
            return (self.words[first_word] & mask).count_ones() as u64;
        }
        let mut count = 0u64;
        // First partial word
        let lo = start % 64;
        count += (self.words[first_word] >> lo).count_ones() as u64;
        // Full middle words
        for w in (first_word + 1)..last_word {
            count += self.words[w].count_ones() as u64;
        }
        // Last partial word
        let hi = end % 64;
        let mask = if hi == 0 { u64::MAX } else { (1u64 << hi) - 1 };
        count += (self.words[last_word] & mask).count_ones() as u64;
        count
    }

    /// Iterate set bit indices in [start, end).
    pub fn iter_ones_range(&self, start: usize, end: usize) -> BitmapOnesRange<'_> {
        let end = end.min(self.len);
        let word_idx = if start < end { start / 64 } else { self.words.len() };
        let last_word = if start < end { (end - 1) / 64 } else { 0 };
        let current_word = if word_idx < self.words.len() {
            // Mask off bits before `start` in the first word
            let mask = u64::MAX << (start % 64);
            self.words[word_idx] & mask
        } else {
            0
        };
        BitmapOnesRange {
            bitmap: self,
            word_idx,
            last_word,
            end,
            current_word,
        }
    }

    fn words(&self) -> &[u64] {
        &self.words
    }
}

pub struct BitmapOnes<'a> {
    bitmap: &'a Bitmap,
    word_idx: usize,
    current_word: u64,
}

impl<'a> Iterator for BitmapOnes<'a> {
    type Item = usize;

    fn next(&mut self) -> Option<Self::Item> {
        loop {
            if self.word_idx >= self.bitmap.words.len() {
                return None;
            }
            if self.current_word != 0 {
                let bit = self.current_word.trailing_zeros() as usize;
                self.current_word &= self.current_word - 1;
                return Some(self.word_idx * 64 + bit);
            }
            self.word_idx += 1;
            self.current_word = self.bitmap.words.get(self.word_idx).copied().unwrap_or(0);
        }
    }
}

pub struct BitmapOnesRange<'a> {
    bitmap: &'a Bitmap,
    word_idx: usize,
    last_word: usize,
    end: usize,
    current_word: u64,
}

impl<'a> Iterator for BitmapOnesRange<'a> {
    type Item = usize;

    fn next(&mut self) -> Option<Self::Item> {
        loop {
            if self.word_idx > self.last_word || self.word_idx >= self.bitmap.words.len() {
                return None;
            }
            if self.current_word != 0 {
                let bit = self.current_word.trailing_zeros() as usize;
                let idx = self.word_idx * 64 + bit;
                self.current_word &= self.current_word - 1;
                if idx < self.end {
                    return Some(idx);
                } else {
                    return None;
                }
            }
            self.word_idx += 1;
            if self.word_idx > self.last_word {
                return None;
            }
            let w = self.bitmap.words.get(self.word_idx).copied().unwrap_or(0);
            // On the last word, mask off bits at or after `end`
            self.current_word = if self.word_idx == self.last_word {
                let hi = self.end % 64;
                let mask = if hi == 0 { u64::MAX } else { (1u64 << hi) - 1 };
                w & mask
            } else {
                w
            };
        }
    }
}

pub fn build_equality_bitmaps(values: &[i64]) -> HashMap<i64, Bitmap> {
    let mut map: HashMap<i64, Bitmap> = HashMap::new();
    for (idx, value) in values.iter().copied().enumerate() {
        map.entry(value)
            .or_insert_with(|| Bitmap::new(values.len()))
            .set(idx);
    }
    map
}

pub fn write_bitmap_index(
    path: &Path,
    row_count: usize,
    values: &[i64],
    bitmaps: &HashMap<i64, Bitmap>,
) -> Result<(), StorageError> {
    let mut ordered = BTreeMap::new();
    for value in values {
        if let Some(bitmap) = bitmaps.get(value) {
            ordered.insert(*value, bitmap.clone());
        }
    }

    let mut bytes = Vec::new();
    bytes.extend_from_slice(&BITMAP_MAGIC);
    bytes.extend_from_slice(&BITMAP_VERSION.to_le_bytes());
    bytes.extend_from_slice(&(row_count as u64).to_le_bytes());
    bytes.extend_from_slice(&(ordered.len() as u32).to_le_bytes());
    let words_per_bitmap = row_count.div_ceil(64);
    for (value, bitmap) in ordered {
        bytes.extend_from_slice(&value.to_le_bytes());
        for word in bitmap.words().iter().take(words_per_bitmap) {
            bytes.extend_from_slice(&word.to_le_bytes());
        }
    }

    fs::write(path, bytes)?;
    Ok(())
}

pub fn read_bitmap_index(path: &Path) -> Result<HashMap<i64, Bitmap>, StorageError> {
    let file = File::open(path)?;
    let mmap = unsafe { Mmap::map(&file).map_err(StorageError::ReadError)? };
    let bytes = mmap.as_ref();
    if bytes.len() < 20 || bytes[..4] != BITMAP_MAGIC {
        return Err(StorageError::InvalidFormat(
            "bitmap index missing BMP1 header".to_string(),
        ));
    }
    let version = u32::from_le_bytes(bytes[4..8].try_into().unwrap());
    if version != BITMAP_VERSION {
        return Err(StorageError::InvalidFormat(format!(
            "unsupported bitmap version {}",
            version
        )));
    }
    let row_count = u64::from_le_bytes(bytes[8..16].try_into().unwrap()) as usize;
    let entry_count = u32::from_le_bytes(bytes[16..20].try_into().unwrap()) as usize;
    let words_per_bitmap = row_count.div_ceil(64);
    let entry_size = 8 + words_per_bitmap * 8;
    let mut offset = 20usize;
    let mut out = HashMap::new();
    for _ in 0..entry_count {
        let end = offset + entry_size;
        if end > bytes.len() {
            return Err(StorageError::InvalidFormat(
                "bitmap index truncated".to_string(),
            ));
        }
        let value = i64::from_le_bytes(bytes[offset..offset + 8].try_into().unwrap());
        offset += 8;
        let mut bitmap = Bitmap::new(row_count);
        for word_idx in 0..words_per_bitmap {
            bitmap.words[word_idx] =
                u64::from_le_bytes(bytes[offset..offset + 8].try_into().unwrap());
            offset += 8;
        }
        out.insert(value, bitmap);
    }
    Ok(out)
}
