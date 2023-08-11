use crate::event_store::column_event_store::encoding::{NonNullableDecoding, NullableDecoding};
use crate::types::{BITS_PER_FLOAT, BITS_PER_INT, FLOAT, INT, UINT};
use bit_vec::BitVec;
use ordered_float::OrderedFloat;
use std::mem::size_of;

#[derive(Debug, Clone)]
pub struct BitPackedIntVec {
    values: BitVec,
    bits_per_value: usize,
}

impl BitPackedIntVec {
    pub fn encode(vec: Vec<INT>) -> Self {
        let bits_per_value = BITS_PER_INT;
        let mut bitvec = BitVec::new();

        for int_value in vec {
            let value_as_uint: UINT = unsafe { std::mem::transmute(int_value) };
            for i in 0..bits_per_value {
                bitvec.push(value_as_uint & (1 << i) != 0);
            }
        }

        Self {
            values: bitvec,
            bits_per_value,
        }
    }
}

impl NonNullableDecoding<INT> for BitPackedIntVec {
    fn decode(self) -> Vec<INT> {
        let num_values = self.values.len() / BITS_PER_INT;
        let mut bit_iter = self.values.into_iter();
        let mut vec = Vec::new();
        let num_bits = 8 * size_of::<UINT>();

        for _ in 0..num_values {
            let mut value_as_uint: UINT = 0;
            for i in 0..num_bits {
                if bit_iter.next().unwrap_or(false) {
                    value_as_uint |= 1 << i;
                }
            }
            let value: INT = unsafe { std::mem::transmute(value_as_uint) };
            vec.push(value);
        }

        vec
    }

    fn size(&self) -> usize {
        let bitvec_size = self.values.len() / 8 + if self.values.len() % 8 > 0 { 1 } else { 0 };
        let bits_per_value_size = std::mem::size_of::<usize>();
        bitvec_size + bits_per_value_size
    }
}

#[derive(Debug, Clone)]
pub struct BitPackedIntVecOption {
    values: BitVec,
    bits_per_value: usize,
}

impl BitPackedIntVecOption {
    pub fn encode(vec: Vec<Option<INT>>) -> Self {
        let bits_per_value = BITS_PER_INT;
        let mut bitvec = BitVec::new();

        for value in vec {
            match value {
                Some(int_value) => {
                    bitvec.push(true); // flag indicating this is Some
                    let value_as_uint: UINT = unsafe { std::mem::transmute(int_value) };
                    for i in 0..bits_per_value {
                        bitvec.push(value_as_uint & (1 << i) != 0);
                    }
                }
                None => {
                    bitvec.push(false); // flag indicating this is None
                }
            }
        }

        Self {
            values: bitvec,
            bits_per_value,
        }
    }
}

impl NullableDecoding<INT> for BitPackedIntVecOption {
    fn decode(self) -> Vec<Option<INT>> {
        let mut vec = Vec::new();
        let mut bit_iter = self.values.iter();

        while let Some(is_some) = bit_iter.next() {
            if is_some {
                let mut value_as_uint = 0 as UINT;
                for i in 0..self.bits_per_value {
                    if bit_iter.next().unwrap() {
                        value_as_uint |= 1 << i;
                    }
                }
                let value: INT = unsafe { std::mem::transmute(value_as_uint) };
                vec.push(Some(value));
            } else {
                vec.push(None);
            }
        }

        vec
    }

    fn size(&self) -> usize {
        let bitvec_size = self.values.len() / 8 + if self.values.len() % 8 > 0 { 1 } else { 0 };
        let bits_per_value_size = std::mem::size_of::<usize>();
        bitvec_size + bits_per_value_size
    }
}

#[cfg(test)]
mod int_tests {
    use super::*;
    use crate::types::{BITS_PER_INT, INT, UINT};

    #[test]
    fn test_bitpackedintvec() {
        let original = vec![10, -20, 300, -4000, 0];

        let encoded = BitPackedIntVec::encode(original.clone());
        let decoded = encoded.decode();

        assert_eq!(original, decoded);
    }

    #[test]
    fn test_bitpackedintvecoption() {
        let original = vec![
            Some(10),
            None,
            Some(-20),
            Some(300),
            None,
            Some(-4000),
            Some(0),
        ];

        let encoded = BitPackedIntVecOption::encode(original.clone());
        let decoded = encoded.decode();

        assert_eq!(original, decoded);
    }

    #[test]
    fn test_bitpackedintvecoption_all_none() {
        let original = vec![None, None];

        let encoded = BitPackedIntVecOption::encode(original.clone());
        let decoded = encoded.decode();

        assert_eq!(original, decoded);
    }

    #[test]
    fn test_bitpackedintvecoption_all_int() {
        let original = vec![Some(10), Some(-20), Some(300), Some(-4000), Some(0)];

        let encoded = BitPackedIntVecOption::encode(original.clone());
        let decoded = encoded.decode();

        assert_eq!(original, decoded);
    }
}
