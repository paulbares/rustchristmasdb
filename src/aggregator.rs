use std::any::Any;
use std::cell::RefCell;
use std::rc::Rc;
use std::sync::Arc;
use arrow::array::{Array, ArrayRef, Float64Builder, PrimitiveArray, UInt64Builder};

use arrow::datatypes::{ArrowPrimitiveType, DataType, Float64Type, UInt32Type, UInt64Type};
use crate::chunk_array::ChunkArray;
use crate::datastore::CHUNK_DEFAULT_SIZE;


pub trait Aggregator {
    fn aggregate(&mut self, source_position: u32, destination_position: u32);

    fn finish(&mut self);

    fn as_any(&self) -> &dyn Any;

    fn get_destination(&self) -> &dyn Array;
}

pub trait AggregatorAccessor<T: ArrowPrimitiveType> {
    fn get_destination(&self) -> &PrimitiveArray<T>;
}

pub struct SumUIntAggregator {
    source: ArrayRef,
    destination: Option<PrimitiveArray<UInt64Type>>,
    buffer: Rc<RefCell<Vec<u64>>>,
}

impl SumUIntAggregator {
    fn new(source: ArrayRef) -> Box<dyn Aggregator> {
        let capacity = CHUNK_DEFAULT_SIZE; // FIXME make it grow when to big
        let mut buffer = Vec::with_capacity(capacity);
        buffer.resize(capacity, 0);
        Box::new(SumUIntAggregator {
            source,
            destination: None,
            buffer: Rc::new(RefCell::new(buffer)),
        })
    }

    fn new_with_buffer(source: ArrayRef, destination: Rc<RefCell<Vec<u64>>>) -> Box<dyn Aggregator> {
        Box::new(SumUIntAggregator {
            source,
            destination: None,
            buffer: destination,
        })
    }

    pub fn get_destination(&self) -> &PrimitiveArray<UInt64Type> {
        self.destination.as_ref().unwrap()
    }
}

impl Aggregator for SumUIntAggregator {
    fn aggregate(&mut self, source_position: u32, destination_position: u32) {
        let a: u64 = read::<UInt32Type>(&self.source, source_position) as u64;
        let b: u64 = match self.buffer.borrow().get(destination_position as usize) {
            None => { 0u64 }
            Some(v) => { *v }
        };
        self.buffer.borrow_mut()[destination_position as usize] = a + b;
    }

    fn finish(&mut self) {
        let mut builder = UInt64Builder::new(CHUNK_DEFAULT_SIZE);
        builder
            .append_slice(self.buffer.borrow().as_slice())
            .unwrap();
        let array: PrimitiveArray<UInt64Type> = builder.finish();
        self.destination = Some(array);
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn get_destination(&self) -> &dyn Array {
        self.get_destination()
    }
}

pub struct SumFloat64Aggregator {
    source: ArrayRef,
    destination: Option<PrimitiveArray<Float64Type>>,
    buffer: Vec<f64>,
}

pub struct SumFloat64Aggregator2 {
    source: ArrayRef,
    buffer: Rc<Vec<f64>>,
}

impl SumFloat64Aggregator {
    fn new(source: ArrayRef) -> Box<dyn Aggregator> {
        // fn new(source: ArrayRef) -> impl Aggregator<T> {
        let capacity = CHUNK_DEFAULT_SIZE; // FIXME make it grow when to big
        let mut buffer = Vec::with_capacity(capacity);
        buffer.resize(capacity, 0f64);
        Box::new(SumFloat64Aggregator {
            source,
            destination: None,
            buffer,
        })
    }

    fn new_with_buffer(source: ArrayRef, destination: Vec<f64>) -> Box<dyn Aggregator> {
        Box::new(SumFloat64Aggregator {
            source,
            destination: None,
            buffer: destination,
        })
    }

    pub fn get_destination(&self) -> &PrimitiveArray<Float64Type> {
        self.destination.as_ref().unwrap()
    }
}

impl Aggregator for SumFloat64Aggregator {
    fn aggregate(&mut self, source_position: u32, destination_position: u32) {
        let a: f64 = read::<Float64Type>(&self.source, source_position);
        let x: Option<&f64> = self.buffer.get(destination_position as usize);
        let b: f64 = match x {
            None => { 0f64 }
            Some(v) => { *v }
        };
        self.buffer[destination_position as usize] = a + b;
    }

    fn finish(&mut self) {
        let mut builder = Float64Builder::new(CHUNK_DEFAULT_SIZE);
        builder
            .append_slice(self.buffer.as_slice())
            .unwrap();
        let array: PrimitiveArray<Float64Type> = builder.finish();
        self.destination = Some(array);
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn get_destination(&self) -> &dyn Array {
        self.get_destination()
    }
}

pub struct AggregatorFactory;

impl AggregatorFactory {
    pub fn new() -> AggregatorFactory {
        AggregatorFactory {}
    }

    pub fn create(&self, source: &ChunkArray, _aggregation_type: &str, _destination_column_name: &str) -> Box<dyn Aggregator> {
        match source.field.data_type() {
            DataType::UInt32 => {
                SumUIntAggregator::new(Arc::clone(source.array.as_ref().unwrap()))
            }
            DataType::Float64 => {
                SumFloat64Aggregator::new(Arc::clone(source.array.as_ref().unwrap()))
            }
            _ => {
                panic!("{} not supported", source.field.data_type())
            }
        }
    }

    pub fn create_with_destination(&self,
                                   source: &ChunkArray,
                                   aggregator: &mut dyn Aggregator,
                                   _aggregation_type: &str) -> Box<dyn Aggregator> {
        match source.field.data_type() {
            DataType::UInt32 => {
                let dest: &SumUIntAggregator = aggregator.as_any().downcast_ref::<SumUIntAggregator>().unwrap();
                // let vec = dest.buffer;
                SumUIntAggregator::new_with_buffer(Arc::clone(source.array.as_ref().unwrap()), Rc::clone(&dest.buffer)) // FIXME
            }
            DataType::Float64 => {
                let _dest: &SumFloat64Aggregator = aggregator.as_any().downcast_ref::<SumFloat64Aggregator>().unwrap();
                SumFloat64Aggregator::new_with_buffer(Arc::clone(source.array.as_ref().unwrap()), Vec::new()) // FIXME
            }
            _ => {
                panic!("{} not supported", source.field.data_type())
            }
        }
    }
}

fn read<T: ArrowPrimitiveType>(array: &ArrayRef, row: u32) -> T::Native {
    let array = array.as_any().downcast_ref::<PrimitiveArray<T>>().unwrap();

    unsafe { array.value_unchecked(row as usize) }
}