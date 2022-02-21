use std::borrow::Borrow;
use crate::chunk_array::ChunkArray;
use crate::row_mapping::{IdentityMapping, RowMapping};
use arrow::array::{Array, ArrayData, ArrayRef, as_boolean_array, Float64Builder, PrimitiveArray, PrimitiveBuilder, StringArray, UInt32Builder, UInt64Array, UInt64Builder};
use arrow::datatypes::{ArrowPrimitiveType, DataType, Field, Float64Type, Schema, SchemaRef, UInt64Type};
use arrow::record_batch::RecordBatch;
use std::cell::RefCell;
use std::collections::HashMap;
use std::fmt::Debug;
use std::ops::Add;
use std::sync::Arc;
use arrow::ipc::{Utf8, Utf8Builder};
use crate::dictionary_provider::{Dictionary, DictionaryProvider};

pub const MAIN_SCENARIO_NAME: &str = "base";

#[derive(Debug)]
pub struct Store {
    pub row_count: RefCell<u64>,
    schema: SchemaRef,
    key_indices: Vec<u32>,
    array_size: u32,
    pub vector_by_field_by_scenario: HashMap<String, HashMap<String, ChunkArray>>,
    row_mapping_by_field_by_scenario:
        RefCell<HashMap<String, HashMap<String, Box<dyn RowMapping>>>>,
    dictionary_provider: DictionaryProvider
}

impl Store {
    pub fn new(schema: Arc<Schema>, key_indices: Vec<u32>, array_size: u32) -> Store {
        let mut vector_by_field_by_scenario = HashMap::new();
        let mut row_mapping_by_field_by_scenario: HashMap<
            String,
            HashMap<String, Box<dyn RowMapping>>,
        > = HashMap::new();
        schema.fields().iter().for_each(|f| {
            let field = f.clone();
            vector_by_field_by_scenario
                .entry(MAIN_SCENARIO_NAME.to_string())
                .or_insert(HashMap::new())
                .entry(field.name().to_string())
                .or_insert_with(|| Store::create_chunk_array(field, array_size));
            row_mapping_by_field_by_scenario
                .entry(MAIN_SCENARIO_NAME.to_string())
                .or_insert(HashMap::new())
                .entry(f.name().clone())
                .or_insert_with(|| Box::new(IdentityMapping::new()));
        });

        Store {
            row_count: RefCell::new(0),
            schema,
            key_indices,
            array_size,
            vector_by_field_by_scenario: vector_by_field_by_scenario,
            row_mapping_by_field_by_scenario: RefCell::new(row_mapping_by_field_by_scenario),
            dictionary_provider: DictionaryProvider::new(),
        }
    }

    fn create_chunk_array(field: Field, array_size: u32) -> ChunkArray {
        ChunkArray::new(field, array_size)
    }

    pub fn load(&mut self, scenario: &str, batch: &RecordBatch) {
        // batch.schema()
        let num_rows = batch.num_rows();
        let batch_size = self.array_size as usize;
        let arc = batch.schema();
        let bucket = num_rows % batch_size;
        let remaining = num_rows / batch_size;
        for i in 0..=bucket {
            let slice = batch.slice(
                i * batch_size,
                if i < bucket { batch_size } else { remaining },
            ); // TODO we can create batch
            for index in 0..slice.columns().len() {
                let col = slice.column(index);

                if index == 0 {
                    let mut rc = self.row_count.borrow_mut();
                    *rc = *rc + col.len() as u64;
                }

                let field = arc.field(index);
                match field.data_type().clone() {
                    DataType::UInt64 => {
                        let builder = UInt64Builder::new(batch_size);
                        self.primitive::<UInt64Type>(batch_size, col, scenario, field, builder);
                    }
                    DataType::Utf8 => {
                        let string_array = col.as_any().downcast_ref::<StringArray>().unwrap();
                        let dic = self.dictionary_provider.dicos
                            .entry(field.name().to_string())
                            .or_insert(Dictionary::new());
                        let mut builder = UInt32Builder::new(string_array.len());
                        for element in string_array {
                            builder.append_value(*dic.map(element.unwrap().to_string()));
                        }
                        self.vector_by_field_by_scenario
                            .get_mut(scenario)
                            .unwrap()
                            .get_mut(field.name())
                            .unwrap()
                            .add_array_primitive(builder.finish());
                    }
                    DataType::Float64 => {
                        let builder = Float64Builder::new(batch_size);
                        self.primitive::<Float64Type>(batch_size, col, scenario, field, builder);
                    }
                    _ => { panic!("type not supported {}", field.data_type())}
                }
            }
        }
    }

    fn primitive<T: ArrowPrimitiveType>(
        &mut self,
        batch_size: usize,
        col: &ArrayRef,
        scenario: &str,
        field: &Field,
        mut builder: PrimitiveBuilder<T>) {
        let arr = col.as_any().downcast_ref::<PrimitiveArray<T>>().unwrap();
        for element in arr.iter() {
            builder.append_value(element.unwrap()).unwrap();
        }
        let array = builder.finish();
        let mut chunk_array = self.vector_by_field_by_scenario
            .get_mut(scenario)
            .unwrap()
            .get_mut(field.name())
            .unwrap();
        chunk_array.add_array_primitive(array);
    }

    pub fn schema(&self) -> Arc<Schema> {
        Arc::clone(&self.schema)
    }

    pub fn show(&self) {
        // self.vector_by_field_by_scenario
    }
}
