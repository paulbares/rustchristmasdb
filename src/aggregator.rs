use arrow::datatypes::{DataType, Field, Float64Type};
use crate::chunk_array::ChunkArray;
use crate::datastore::CHUNK_DEFAULT_SIZE;

trait Aggregator {
    fn aggregate(&self, source_position: u32, destination_position: u32);

    fn get_destination(&self) -> &ChunkArray;
}

struct SumAggregator<'a> {
    source: &'a ChunkArray,
    destination: ChunkArray,
}

struct SumFloat64Aggregator<'a> {
    source: &'a ChunkArray,
    destination: ChunkArray,
}

impl Aggregator for SumFloat64Aggregator<'_> {
    fn aggregate(&self, source_position: u32, destination_position: u32) {
        let a: f64 = self.source.read::<Float64Type>(source_position);
        let b: f64 = self.destination.read::<Float64Type>(source_position);
        // self.destination.set_float(64)
    }

    fn get_destination(&self,) -> &ChunkArray {
        &self.destination
    }
}

struct AggregatorFactory {}

impl AggregatorFactory {
    pub fn new() -> AggregatorFactory {
        AggregatorFactory {}
    }

    pub fn create(source: &ChunkArray, aggregation_type: &str, destination_column_name: &str) {
        // suppport only float64
        let field = Field::new(destination_column_name, DataType::Float64, false);
        let array = ChunkArray::new(field, CHUNK_DEFAULT_SIZE);
    }
}