use std::sync::Arc;
use arrow::array::{Array, PrimitiveArray, PrimitiveBuilder, StringArray, UInt32Builder};
use arrow::buffer::{Buffer, MutableBuffer};
use arrow::datatypes::{ArrowPrimitiveType, Field, Int32Type, UInt64Type};

#[derive(Debug)]
pub struct ChunkArray {
    field: Field,
    arrays: Vec<Arc<dyn Array>>,
}

impl ChunkArray {
    pub fn new(field: Field, size: u32) -> ChunkArray {
        if !ChunkArray::is_power_of_two(size) {
            panic!("{} not a power of 2", size);
        }

        // let v: Vec<Arc<Box<dyn Array>>> = vec![];
        let v: Vec<Arc<dyn Array>> = vec![];
        ChunkArray {
            field,
            arrays: v,
        }
    }

    pub fn add_array(&mut self, array: PrimitiveArray<UInt64Type>) {
        self.arrays.push(Arc::new(array));
        // self.arrays.push(Arc::new(Box::new(array)));
    }

    pub fn add_array_primitive<T: ArrowPrimitiveType>(&mut self, array: PrimitiveArray<T>) {
        self.arrays.push(Arc::new(array));
        // self.arrays.push(Arc::new(Box::new(array)));
    }

    pub fn add_array_string(&mut self, array: StringArray) {
        self.arrays.push(Arc::new(array));
        // self.arrays.push(Arc::new(Box::new(array)));
    }

    fn is_power_of_two(number: u32) -> bool {
        number > 0 && ((number & (number - 1)) == 0)
    }

    pub fn append_uint64(&mut self, v: u64) {

    }
}
