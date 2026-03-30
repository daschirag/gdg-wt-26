use std::ops::{Deref, DerefMut};
use serde::{Serialize, Deserialize, Serializer, Deserializer};
use serde::de::{Visitor, SeqAccess};
use std::marker::PhantomData;

use std::alloc::{alloc, dealloc, Layout, handle_alloc_error};
use std::ptr;

/// A simple Vec-like structure that ensures memory is aligned for SIMD operations.
/// Default alignment is 64 bytes (suitable for AVX-512).
pub struct AlignedVec<T> {
    ptr: *mut T,
    len: usize,
    cap: usize,
}

impl<T: Serialize> Serialize for AlignedVec<T> {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        self.as_slice().serialize(serializer)
    }
}

impl<'de, T: Deserialize<'de>> Deserialize<'de> for AlignedVec<T> {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        struct AlignedVecVisitor<T>(PhantomData<T>);

        impl<'de, T: Deserialize<'de>> Visitor<'de> for AlignedVecVisitor<T> {
            type Value = AlignedVec<T>;

            fn expecting(&self, formatter: &mut std::fmt::Formatter) -> std::fmt::Result {
                formatter.write_str("a sequence")
            }

            fn visit_seq<A>(self, mut seq: A) -> Result<Self::Value, A::Error>
            where
                A: SeqAccess<'de>,
            {
                let mut vec = AlignedVec::with_capacity(seq.size_hint().unwrap_or(0));
                while let Some(element) = seq.next_element()? {
                    vec.push(element);
                }
                Ok(vec)
            }
        }

        deserializer.deserialize_seq(AlignedVecVisitor(PhantomData))
    }
}

impl<T> AlignedVec<T> {
    const ALIGN: usize = 64;

    pub fn new() -> Self {
        Self::with_capacity(0)
    }

    pub fn with_capacity(capacity: usize) -> Self {
        if capacity == 0 {
            return Self {
                ptr: ptr::NonNull::<T>::dangling().as_ptr(),
                len: 0,
                cap: 0,
            };
        }

        let layout = Layout::from_size_align(
            capacity * std::mem::size_of::<T>(),
            Self::ALIGN
        ).expect("Invalid layout");

        let ptr = unsafe { alloc(layout) as *mut T };
        if ptr.is_null() {
            handle_alloc_error(layout);
        }

        Self {
            ptr,
            len: 0,
            cap: capacity,
        }
    }

    pub fn from_slice(slice: &[T]) -> Self where T: Clone {
        let mut av = Self::with_capacity(slice.len());
        for item in slice {
            av.push(item.clone());
        }
        av
    }

    pub fn push(&mut self, value: T) {
        if self.len == self.cap {
            self.grow();
        }
        unsafe {
            ptr::write(self.ptr.add(self.len), value);
        }
        self.len += 1;
    }

    fn grow(&mut self) {
        let new_cap = if self.cap == 0 { 8 } else { self.cap * 2 };
        let new_layout = Layout::from_size_align(
            new_cap * std::mem::size_of::<T>(),
            Self::ALIGN
        ).expect("Invalid layout");

        let new_ptr = if self.cap == 0 {
            unsafe { alloc(new_layout) as *mut T }
        } else {
            let old_layout = Layout::from_size_align(
                self.cap * std::mem::size_of::<T>(),
                Self::ALIGN
            ).expect("Invalid layout");
            unsafe {
                let p = alloc(new_layout) as *mut T;
                if !p.is_null() {
                    ptr::copy_nonoverlapping(self.ptr, p, self.len);
                    dealloc(self.ptr as *mut u8, old_layout);
                }
                p
            }
        };

        if new_ptr.is_null() {
            handle_alloc_error(new_layout);
        }

        self.ptr = new_ptr;
        self.cap = new_cap;
    }

    pub fn len(&self) -> usize {
        self.len
    }

    pub fn as_slice(&self) -> &[T] {
        unsafe { std::slice::from_raw_parts(self.ptr, self.len) }
    }

    pub fn as_mut_slice(&mut self) -> &mut [T] {
        unsafe { std::slice::from_raw_parts_mut(self.ptr, self.len) }
    }
}

impl<T> Deref for AlignedVec<T> {
    type Target = [T];
    fn deref(&self) -> &Self::Target {
        self.as_slice()
    }
}

impl<T> DerefMut for AlignedVec<T> {
    fn deref_mut(&mut self) -> &mut [T] {
        self.as_mut_slice()
    }
}

impl<T> Drop for AlignedVec<T> {
    fn drop(&mut self) {
        if self.cap != 0 {
            // Drop elements
            for i in 0..self.len {
                unsafe {
                    ptr::drop_in_place(self.ptr.add(i));
                }
            }
            // Deallocate memory
            let layout = Layout::from_size_align(
                self.cap * std::mem::size_of::<T>(),
                Self::ALIGN
            ).expect("Invalid layout");
            unsafe {
                dealloc(self.ptr as *mut u8, layout);
            }
        }
    }
}

// Safety: Manually implementing Send/Sync
unsafe impl<T: Send> Send for AlignedVec<T> {}
unsafe impl<T: Sync> Sync for AlignedVec<T> {}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_alignment() {
        let av = AlignedVec::<i64>::from_slice(&[1, 2, 3, 4]);
        let addr = av.ptr as usize;
        assert_eq!(addr % 64, 0);
        assert_eq!(av.len(), 4);
        assert_eq!(av[0], 1);
        assert_eq!(av[3], 4);
    }
}
