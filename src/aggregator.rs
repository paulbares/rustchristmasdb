use std::sync::Arc;
use arrow::array::{Array, ArrayData, ArrayDataBuilder, ArrayRef, Float64Builder, make_array, PrimitiveArray, PrimitiveBuilder};
use arrow::buffer::Buffer;
use arrow::datatypes::{ArrowPrimitiveType, DataType, Field, Float64Type};
use crate::chunk_array::ChunkArray;
use crate::datastore::CHUNK_DEFAULT_SIZE;

pub trait Aggregator<T: ArrowPrimitiveType> {
    fn aggregate(&mut self, source_position: u32, destination_position: u32);

    fn get_destination(&self) -> &PrimitiveArray<T>;

    fn finish(&mut self);
}

pub struct SumFloat64Aggregator<T: ArrowPrimitiveType> {
    source: ArrayRef,
    destination: Option<PrimitiveArray<T>>,
    buffer: Vec<f64>,
}

impl<T: ArrowPrimitiveType> SumFloat64Aggregator<T> {
    fn new(source: ArrayRef) -> SumFloat64Aggregator<T> {
        let capacity = 4; // FIXME make it grow when to big
        let mut buffer = Vec::with_capacity(capacity);
        buffer.resize(capacity, 0f64);
        SumFloat64Aggregator {
            source,
            destination: None,
            buffer
        }
    }
}

impl Aggregator<Float64Type> for SumFloat64Aggregator<Float64Type> {
    fn aggregate(&mut self, source_position: u32, destination_position: u32) {
        let a: f64 = read::<Float64Type>(&self.source, source_position);
        let x: Option<&f64> = self.buffer.get(destination_position as usize);
        let b: f64 = match x {
            None => { 0f64 }
            Some(v) => { *v }
        };
        self.buffer[destination_position as usize] = a + b;
    }

    fn get_destination(&self) -> &PrimitiveArray<Float64Type> {
        self.destination.as_ref().unwrap()
    }

    fn finish(&mut self) {
        // let buffer = Buffer::from_iter(self.buffer.as_slice());
        let mut builder = Float64Builder::new(4);
        builder
            .append_slice(self.buffer.as_slice())
            .unwrap();
        let array: PrimitiveArray<Float64Type> = builder.finish();
        self.destination = Some(array);
    }
}

pub struct AggregatorFactory {}

impl AggregatorFactory {
    pub fn new() -> AggregatorFactory {
        AggregatorFactory {}
    }

    pub fn create<T: ArrowPrimitiveType>(&self, source: &ChunkArray, aggregation_type: &str, destination_column_name: &str) -> SumFloat64Aggregator<T> {
        // suppport only float64
        // let field = Field::new(destination_column_name, DataType::Float64, false);
        let aggregator: SumFloat64Aggregator<T> = SumFloat64Aggregator::new(Arc::clone(source.array.as_ref().unwrap()));
        // let p: Box<dyn Aggregator<T>> = Box::new(aggregator);
        aggregator
    }
}

fn read<T: ArrowPrimitiveType>(array: &ArrayRef, row: u32) -> T::Native {
    let array = array.as_any().downcast_ref::<PrimitiveArray<T>>().unwrap();
    let result = unsafe { array.value_unchecked(row as usize) };
    result
}