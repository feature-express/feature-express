use crate::event_store::column_event_store::encoding::{NonNullableDecoding, NullableDecoding};
use crate::types::{BITS_PER_FLOAT, BITS_PER_INT, FLOAT, UINT};
use bit_vec::BitVec;
use ordered_float::OrderedFloat;
use std::mem::size_of;

#[derive(Debug, Clone)]
pub struct BitPackedFloatVecOption {
    values: BitVec,
    bits_per_value: usize,
}

impl BitPackedFloatVecOption {
    pub fn encode(vec: Vec<Option<OrderedFloat<FLOAT>>>) -> Self {
        let bits_per_value = BITS_PER_FLOAT;
        let mut bitvec = BitVec::new();

        for value in vec {
            match value {
                Some(float_value) => {
                    bitvec.push(true); // flag indicating this is Some
                    let value_as_int: UINT = unsafe { std::mem::transmute(float_value.0) };
                    for i in 0..bits_per_value {
                        bitvec.push(value_as_int & (1 << i) != 0);
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

impl NullableDecoding<OrderedFloat<FLOAT>> for BitPackedFloatVecOption {
    fn decode(self) -> Vec<Option<OrderedFloat<FLOAT>>> {
        let mut vec = Vec::new();
        let mut bit_iter = self.values.iter();

        while let Some(is_some) = bit_iter.next() {
            if is_some {
                let mut value_as_int = 0 as UINT;
                for i in 0..self.bits_per_value {
                    if bit_iter.next().unwrap() {
                        value_as_int |= 1 << i;
                    }
                }
                let value: FLOAT = unsafe { std::mem::transmute(value_as_int) };
                vec.push(Some(OrderedFloat(value)));
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

#[derive(Debug, Clone)]
pub struct BitPackedFloatVec {
    values: BitVec,
    bits_per_value: usize,
}

impl BitPackedFloatVec {
    pub fn encode(vec: Vec<OrderedFloat<FLOAT>>) -> Self {
        let bits_per_value = BITS_PER_FLOAT;
        let mut bitvec = BitVec::new();

        for float_value in vec {
            let value_as_int: UINT = unsafe { std::mem::transmute(float_value.0) };
            for i in 0..bits_per_value {
                bitvec.push(value_as_int & (1 << i) != 0);
            }
        }

        Self {
            values: bitvec,
            bits_per_value,
        }
    }
}

impl NonNullableDecoding<OrderedFloat<FLOAT>> for BitPackedFloatVec {
    fn decode(self) -> Vec<OrderedFloat<FLOAT>> {
        let num_values = self.values.len() / BITS_PER_INT;
        let mut bit_iter = self.values.into_iter();
        let mut vec = Vec::new();
        let num_bits = 8 * size_of::<UINT>();

        for _ in 0..num_values {
            let mut value_as_int: UINT = 0;
            for i in 0..num_bits {
                if bit_iter.next().unwrap_or(false) {
                    value_as_int |= 1 << i;
                }
            }
            let value: FLOAT = unsafe { std::mem::transmute(value_as_int) };
            vec.push(OrderedFloat(value));
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
mod tests {
    use super::*;

    #[test]
    fn test_bitpackedvec() {
        let original = vec![
            OrderedFloat(3.14),
            OrderedFloat(-2.71),
            OrderedFloat(0.0),
            OrderedFloat(150.5),
        ];

        let encoded = BitPackedFloatVec::encode(original.clone());
        let decoded = encoded.decode();

        assert_eq!(original, decoded);
    }

    #[test]
    fn test_bitpackedvecoption() {
        let original = vec![
            Some(OrderedFloat(3.14)),
            None,
            Some(OrderedFloat(-2.71)),
            Some(OrderedFloat(0.0)),
            None,
            Some(OrderedFloat(150.5)),
        ];

        let encoded = BitPackedFloatVecOption::encode(original.clone());
        let decoded = encoded.decode();

        assert_eq!(original, decoded);
    }

    #[test]
    fn test_bitpackedvecoption_all_none() {
        let original = vec![None, None];

        let encoded = BitPackedFloatVecOption::encode(original.clone());
        let decoded = encoded.decode();

        assert_eq!(original, decoded);
    }

    #[test]
    fn test_bitpackedvecoption_all_float() {
        let original = vec![
            Some(OrderedFloat(3.14)),
            Some(OrderedFloat(-2.71)),
            Some(OrderedFloat(0.0)),
            Some(OrderedFloat(150.5)),
        ];

        let encoded = BitPackedFloatVecOption::encode(original.clone());
        let decoded = encoded.decode();

        assert_eq!(original, decoded);
    }
}
