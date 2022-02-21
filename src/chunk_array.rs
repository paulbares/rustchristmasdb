use std::sync::Arc;
use arrow::array::{Array, PrimitiveArray, PrimitiveBuilder, StringArray, UInt32Builder};
use arrow::buffer::{Buffer, MutableBuffer};
use arrow::datatypes::{ArrowPrimitiveType, Field, Int32Type, UInt64Type};

#[derive(Debug)]
pub struct ChunkArray {
    field: Field,
    arrays: Vec<Arc<dyn Array>>
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
        // let builder = PrimitiveBuilder::<Int32Type>::new(10);
        // let mut primitive_array_builder = UInt32Builder::new(size as usize);
        // primitive_array_builder.append_values(&vec![2, 3], &vec![true, true]);

        // primitive_array_builder.append_value(4);
        // println!("{:?}", primitive_array_builder);
        // let primitive_array = primitive_array_builder.finish();
        // Long arrays will have an ellipsis printed in the middle
        // println!("{:?}", primitive_array);
        // primitive_array_builder.append_value(8);
        // let array = primitive_array_builder.finish();
        // println!("{:?}", primitive_array);
        // println!("{:?}", array);

        // let mut buffer = MutableBuffer::new(size as usize);
        // unsafe { buffer.push_unchecked(0); }
        // unsafe { buffer.push_unchecked(1); }
        // unsafe { buffer.push_unchecked(2); }
        // unsafe { buffer.push_unchecked(3); }
        // unsafe { buffer.push_unchecked(5); }
        // for i in 0..=64 {
        //     unsafe { buffer.push_unchecked(i); }
        // }
        // println!("{:?}", buffer);
        // let builder = ArrayData::builder(field.data_type().clone())
        //     .len(size as usize)
        //     .add_buffer(Buffer::from(buffer));
        // println!("{:?}", builder);
        // builder.
        // let data = builder.build().unwrap();
        // let data = unsafe { builder.build_unchecked() };
        // println!("{:?}", data);
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
}
