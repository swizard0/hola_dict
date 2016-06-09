use std::default::Default;
use std::hash::Hasher;

#[derive(Debug)]
pub struct FnvHasher32(u32);

impl Default for FnvHasher32 {
    #[inline]
    fn default() -> FnvHasher32 {
        FnvHasher32(0x811C9DC5)
    }
}

impl Hasher for FnvHasher32 {
    #[inline]
    fn finish(&self) -> u64 {
        self.0 as u64
    }

    #[inline]
    fn write(&mut self, bytes: &[u8]) {
        let FnvHasher32(mut hash) = *self;
        for &byte in bytes.iter() {
	    hash = hash ^ (byte as u32);
	    let hash_a = hash.wrapping_shl(24);
            let hash_b = hash.wrapping_shl(8);
            let hash_c = hash.wrapping_shl(7);
            let hash_d = hash.wrapping_shl(4);
            let hash_e = hash.wrapping_shl(1);
            hash = hash.wrapping_add(hash_a);
            hash = hash.wrapping_add(hash_b);
            hash = hash.wrapping_add(hash_c);
            hash = hash.wrapping_add(hash_d);
            hash = hash.wrapping_add(hash_e);
        }

        *self = FnvHasher32(hash);
    }
}
