use std::any::Any;
use std::rc::Rc;
use std::sync::Arc;
use arrow::array::{Array, ArrayRef, PrimitiveArray, PrimitiveBuilder, StringArray, UInt32Builder};
use arrow::buffer::{Buffer, MutableBuffer};
use arrow::datatypes::{ArrowPrimitiveType, Field, Int32Type, UInt64Type};

#[derive(Debug)]
pub struct ChunkArray {
    field: Field,
    pub array: Option<ArrayRef>,
}

impl ChunkArray {
    pub fn new0(field: Field, array: ArrayRef) -> ChunkArray {
        ChunkArray {
            field,
            array: Some(array),
        }
    }

    pub fn new(field: Field, size: u32) -> ChunkArray {
        if !ChunkArray::is_power_of_two(size) {
            panic!("{} not a power of 2", size);
        }

        ChunkArray {
            field,
            array: None,
        }
    }

    pub fn read<T: ArrowPrimitiveType>(&self, row: u32) -> T::Native {
        let array = self.array.as_ref().unwrap().as_any().downcast_ref::<PrimitiveArray<T>>().unwrap();
        let result = unsafe { array.value_unchecked(row as usize) };
        result
    }

    // pub fn set(&self, row: u32) {
    //     let bucket: usize = (row >> self.vector_size.trailing_zeros()) as usize;
    //     let offset: usize = (row & (self.vector_size - 1)) as usize;
    //
        // let array = self.arrays[bucket].as_any().downcast_ref::<PrimitiveArray<T>>().unwrap();
        // array.
    // }

    // pub fn add_array(&mut self, array: PrimitiveArray<UInt64Type>) {
    //     self.arrays.push(Arc::new(array));
    //     // self.arrays.push(Arc::new(Box::new(array)));
    // }
    //
    // pub fn add_array_primitive<T: ArrowPrimitiveType>(&mut self, array: PrimitiveArray<T>) {
    //     self.arrays.push(Arc::new(array));
    //     // self.arrays.push(Arc::new(Box::new(array)));
    // }

    pub fn set_array(&mut self, array: Box<dyn Array>) {
        self.array = Some(ArrayRef::from(array));
    }

    // pub fn add_array_string(&mut self, array: StringArray) {
    //     self.arrays.push(Arc::new(array));
    //     // self.arrays.push(Arc::new(Box::new(array)));
    // }

    fn is_power_of_two(number: u32) -> bool {
        number > 0 && ((number & (number - 1)) == 0)
    }
}
