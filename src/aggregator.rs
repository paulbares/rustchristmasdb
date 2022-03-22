use std::any::Any;
use std::borrow::Borrow;
use std::cell::RefCell;
use std::rc::Rc;
use std::sync::Arc;
use arrow::array::{Array, ArrayRef, Float64Builder, PrimitiveArray, UInt64Builder};

use arrow::datatypes::{ArrowPrimitiveType, DataType, Field, Float64Type, UInt32Type, UInt64Type};
use crate::chunk_array::{ChunkArray, ChunkArrayReader};
use crate::datastore::CHUNK_DEFAULT_SIZE;


pub trait Aggregator {
    fn aggregate(&mut self, source_position: u32, destination_position: u32);

    fn finish(&mut self);

    fn ensure_capacity(&self, destination_position: usize);

    fn as_any(&self) -> &dyn Any;

    fn get_destination(&self) -> &dyn Array;

    fn get_field(&self) -> &Field;
}

pub trait AggregatorAccessor<T: ArrowPrimitiveType> {
    fn get_destination(&self) -> &PrimitiveArray<T>;
}

pub struct SumUIntAggregator {
    source: Arc<ChunkArrayReader>,
    destination: Option<PrimitiveArray<UInt64Type>>,
    buffer: Rc<RefCell<Vec<u64>>>,
    field: Field,
}

impl SumUIntAggregator {
    fn new(source: Arc<ChunkArrayReader>, field: Field) -> Box<dyn Aggregator> {
        let capacity = CHUNK_DEFAULT_SIZE; // FIXME make it grow when to big
        let mut buffer = Vec::with_capacity(capacity);
        buffer.resize(capacity, 0);
        Box::new(SumUIntAggregator {
            source: Arc::clone(&source),
            destination: None,
            buffer: Rc::new(RefCell::new(buffer)),
            field,
        })
    }

    fn new_with_buffer(source: Arc<ChunkArrayReader>, field: Field, destination: Rc<RefCell<Vec<u64>>>) -> Box<dyn Aggregator> {
        Box::new(SumUIntAggregator {
            source: Arc::clone(&source),
            destination: None,
            buffer: destination,
            field,
        })
    }

    pub fn get_destination(&self) -> &PrimitiveArray<UInt64Type> {
        self.destination.as_ref().unwrap()
    }
}

impl Aggregator for SumUIntAggregator {
    fn aggregate(&mut self, source_position: u32, destination_position: u32) {
        let a: u64 = self.source.read::<UInt32Type>(source_position) as u64;
        let buff = &*self.buffer;
        let b: u64 = match buff.borrow().get(destination_position as usize) {
            None => { 0u64 }
            Some(v) => { *v }
        };
        self.buffer.borrow_mut()[destination_position as usize] = a + b;
    }

    fn finish(&mut self) {
        let mut builder = UInt64Builder::new(CHUNK_DEFAULT_SIZE);
        let buff = &*self.buffer;
        builder
            .append_slice(buff.borrow().as_slice())
            .unwrap();
        self.destination = Some(builder.finish());
    }

    fn ensure_capacity(&self, destination_position: usize) {
        let buff = &*self.buffer;
        let len = buff.borrow().len();
        if destination_position >= len {
            buff.borrow_mut().resize(len + CHUNK_DEFAULT_SIZE, 0);
        }
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn get_destination(&self) -> &dyn Array {
        self.get_destination()
    }

    fn get_field(&self) -> &Field {
        &self.field
    }
}

pub struct SumFloat64Aggregator {
    source: Arc<ChunkArrayReader>,
    destination: Option<PrimitiveArray<Float64Type>>,
    buffer: Rc<RefCell<Vec<f64>>>,
    field: Field,
}

impl SumFloat64Aggregator {
    fn new(source: Arc<ChunkArrayReader>, field: Field) -> Box<dyn Aggregator> {
        let capacity = CHUNK_DEFAULT_SIZE; // FIXME make it grow when to big
        let mut buffer = Vec::with_capacity(capacity);
        buffer.resize(capacity, 0f64);
        Box::new(SumFloat64Aggregator {
            source,
            destination: None,
            buffer: Rc::new(RefCell::new(buffer)),
            field,
        })
    }

    fn new_with_buffer(source: Arc<ChunkArrayReader>, field: Field, destination: Rc<RefCell<Vec<f64>>>) -> Box<dyn Aggregator> {
        Box::new(SumFloat64Aggregator {
            source,
            destination: None,
            buffer: destination,
            field,
        })
    }

    pub fn get_destination(&self) -> &PrimitiveArray<Float64Type> {
        self.destination.as_ref().unwrap()
    }
}

impl Aggregator for SumFloat64Aggregator {
    fn aggregate(&mut self, source_position: u32, destination_position: u32) {
        let a: f64 = self.source.read::<Float64Type>(source_position);
        let buff = &*self.buffer;
        let b: f64 = match buff.borrow().get(destination_position as usize) {
            None => { 0f64 }
            Some(v) => { *v }
        };
        buff.borrow_mut()[destination_position as usize] = a + b;
    }

    fn finish(&mut self) {
        let mut builder = Float64Builder::new(CHUNK_DEFAULT_SIZE);
        let buff = &*self.buffer;
        builder
            .append_slice(buff.borrow().as_slice())
            .unwrap();
        let array: PrimitiveArray<Float64Type> = builder.finish();
        self.destination = Some(array);
    }

    fn ensure_capacity(&self, destination_position: usize) {
        let buff = &*self.buffer;
        let len = buff.borrow().len();
        if destination_position >= len {
            buff.borrow_mut().resize(len + CHUNK_DEFAULT_SIZE, 0f64);
        }
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn get_destination(&self) -> &dyn Array {
        self.get_destination()
    }

    fn get_field(&self) -> &Field {
        &self.field
    }
}

pub struct AggregatorFactory;

impl AggregatorFactory {
    pub fn new() -> AggregatorFactory {
        AggregatorFactory {}
    }

    pub fn create(&self, source: Arc<ChunkArrayReader>, _aggregation_type: &str, destination_column_name: &str) -> Box<dyn Aggregator> {
        let data_type = source.data_type();
        match data_type {
            DataType::UInt32 => {
                let field = Field::new(destination_column_name, DataType::UInt64, false);
                SumUIntAggregator::new(Arc::clone(&source), field)
            }
            DataType::Float64 => {
                let field = Field::new(destination_column_name, DataType::Float64, false);
                SumFloat64Aggregator::new(Arc::clone(&source), field)
            }
            _ => panic!("{} not supported", data_type),
        }
    }

    pub fn create_with_destination(&self,
                                   source: Arc<ChunkArrayReader>,
                                   aggregator: &dyn Aggregator,
                                   _aggregation_type: &str) -> Box<dyn Aggregator> {
        let data_type = source.data_type();
        match data_type {
            DataType::UInt32 => {
                let dest: &SumUIntAggregator = aggregator.as_any().downcast_ref::<SumUIntAggregator>().unwrap();
                SumUIntAggregator::new_with_buffer(Arc::clone(&source),
                                                   aggregator.get_field().clone(),
                                                   Rc::clone(&dest.buffer))
            }
            DataType::Float64 => {
                let dest: &SumFloat64Aggregator = aggregator.as_any().downcast_ref::<SumFloat64Aggregator>().unwrap();
                SumFloat64Aggregator::new_with_buffer(Arc::clone(&source),
                                                      aggregator.get_field().clone(),
                                                      Rc::clone(&dest.buffer))
            }
            _ => panic!("{} not supported", data_type),
        }
    }
}

// fn read<T: ArrowPrimitiveType>(array: &ChunkArrayReader, row: u32) -> T::Native {
//     let array = array.as_any().downcast_ref::<PrimitiveArray<T>>().unwrap();
//
//     unsafe { array.value_unchecked(row as usize) }
// }