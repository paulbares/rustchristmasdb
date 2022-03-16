use std::borrow::BorrowMut;
use crate::chunk_array::{ChunkArray, ChunkArrayReader};
use crate::row_mapping::{IdentityMapping, IntIntMapRowMapping, RowMapping};
use arrow::array::{Array, ArrayBuilder, ArrayRef, Float64Builder, Int64Array, Int64Builder, PrimitiveArray, PrimitiveBuilder, StringArray, UInt32Array, UInt32Builder, UInt64Array, UInt64Builder};
use arrow::datatypes::{ArrowPrimitiveType, DataType, Field, Float64Type, Int64Type, Schema, SchemaRef, UInt32Type, UInt64Type};
use arrow::record_batch::RecordBatch;
use std::cell::RefCell;
use std::collections::HashMap;
use std::fmt::Debug;


use std::sync::Arc;
use crate::chunk_array::ChunkArrayReader::{BaseReader, ScenarioReader};


use crate::dictionary_provider::{Dictionary, DictionaryProvider};

pub const MAIN_SCENARIO_NAME: &str = "base";
pub const SCENARIO_FIELD_NAME: &str = "scenario";
pub const CHUNK_DEFAULT_SIZE: usize = 4;

#[derive(Debug)]
pub struct Store {
    pub row_count: RefCell<u64>,
    schema: SchemaRef,
    key_indices: Vec<u32>,
    array_size: u32,
    pub vector_by_field_by_scenario: HashMap<String, HashMap<String, Arc<ChunkArray>>>,
    pub row_mapping_by_field_by_scenario: HashMap<String, HashMap<String, Arc<dyn RowMapping>>>,
    pub dictionary_provider: DictionaryProvider,
    pub primary_index: HashMap<i64, u64>, // FIXME suppose the key is a i64. Should be generic.
}

impl Store {
    pub fn new(schema: Arc<Schema>, key_indices: Vec<u32>, array_size: u32) -> Store {
        let mut vector_by_field_by_scenario = HashMap::new();
        let mut row_mapping_by_field_by_scenario: HashMap<
            String,
            HashMap<String, Arc<dyn RowMapping>>,
        > = HashMap::new();
        schema.fields().iter().for_each(|f| {
            let field = f.clone();
            vector_by_field_by_scenario
                .entry(MAIN_SCENARIO_NAME.to_string())
                .or_insert(HashMap::new())
                .entry(field.name().to_string())
                .or_insert_with(|| Arc::new(Store::create_chunk_array(field, array_size)));
            row_mapping_by_field_by_scenario
                .entry(MAIN_SCENARIO_NAME.to_string())
                .or_insert(HashMap::new())
                .entry(f.name().clone())
                .or_insert_with(|| Arc::new(IdentityMapping::new()));
        });

        Store {
            row_count: RefCell::new(0),
            schema,
            key_indices,
            array_size,
            vector_by_field_by_scenario,
            row_mapping_by_field_by_scenario,
            dictionary_provider: DictionaryProvider::new(),
            primary_index: HashMap::new(),
        }
    }

    pub fn get_scenario_chunk_array(&self, scenario: &str, field: &str) -> ChunkArrayReader {
        let base_array = self.vector_by_field_by_scenario.get(MAIN_SCENARIO_NAME).unwrap().get(field);
        let scenario_array = self.vector_by_field_by_scenario.get(scenario).unwrap().get(field);
        match scenario_array {
            None => {
                BaseReader {
                    base_array: Arc::clone(base_array.unwrap())
                }
            }
            Some(array) => {
                let mapping = self.row_mapping_by_field_by_scenario.get(scenario).unwrap().get(field).unwrap();
                ScenarioReader {
                    base_array: Arc::clone(base_array.unwrap()),
                    scenario: String::from(scenario),
                    scenario_array: Arc::clone(array),
                    row_mapping: Arc::clone(mapping),
                }
            }
        }
    }

    pub fn get_dictionary(&self, field: &str) -> &Dictionary<String> {
        self.dictionary_provider.dicos
            .get(field)
            .expect(format!("cannot find dictionary for field '{}'", field).as_str())
    }

    fn create_chunk_array(field: Field, array_size: u32) -> ChunkArray {
        ChunkArray::new(field, array_size)
    }

    pub fn load(&mut self, scenario: &str, batch: &RecordBatch) {
        let dic = self.dictionary_provider.dicos
            .entry(SCENARIO_FIELD_NAME.to_string())
            .or_insert(Dictionary::new());
        let _ = *dic.map(scenario.to_string());

        if scenario == MAIN_SCENARIO_NAME {
            self.load_main_scenario(scenario, batch);
        } else {
            let key_col = batch.column(self.key_indices[0] as usize);
            for index in 0..batch.columns().len() {
                let col = batch.column(index);
                let schema = batch.schema();
                let field = schema.field(index);

                match field.data_type() {
                    DataType::UInt64 => {
                        self.build_scenario_array(col, key_col, scenario, field, UInt64Builder::new(self.array_size as usize));
                    }
                    DataType::UInt32 => {
                        self.build_scenario_array(col, key_col, scenario, field, UInt32Builder::new(self.array_size as usize));
                    }
                    DataType::Int64 => {
                        self.build_scenario_array(col, key_col, scenario, field, Int64Builder::new(self.array_size as usize));
                    }
                    DataType::Float64 => {
                        self.build_scenario_array(col, key_col, scenario, field, Float64Builder::new(self.array_size as usize));
                    }
                    DataType::Utf8 => {
                        let row_mapping = IntIntMapRowMapping::new();
                        let arr = col.as_any().downcast_ref::<StringArray>().unwrap();
                        let key_arr = key_col.as_any().downcast_ref::<Int64Array>().unwrap();

                        let mut builder = UInt32Builder::new(self.array_size as usize);
                        {
                            let base_vector = self.vector_by_field_by_scenario.get(MAIN_SCENARIO_NAME).unwrap().get(field.name()).unwrap();
                            let array_ref = base_vector.array.borrow();
                            let base_arr = array_ref.as_ref().unwrap().as_any().downcast_ref::<UInt32Array>().unwrap();

                            let dictionary = self.dictionary_provider.dicos
                                .get_mut(field.name())
                                .expect(format!("cannot find dictionary for field '{}'", field).as_str());

                            let mut cursor: u32 = 0;
                            for i in 0..arr.len() {
                                let key = key_arr.value(i);
                                let row = self.primary_index
                                    .get(&key)
                                    .expect(format!("Cannot find key {} in {} scenario", key, MAIN_SCENARIO_NAME).as_str());
                                let row = *row as usize;
                                let original_value = base_arr.value(row);
                                let value = arr.value(i);
                                let position = dictionary.get_position(&value.to_string());
                                match position {
                                    None => {
                                        // should be mapped
                                        let p = dictionary.map(value.to_string());
                                        builder.append_value(*p).unwrap();
                                        row_mapping.map(row as u32, cursor);
                                        cursor += 1;
                                    }
                                    Some(p) => {
                                        if *p != original_value {
                                            // already in dictionary but current value is different
                                            builder.append_value(*p).unwrap();
                                            row_mapping.map(row as u32, cursor);
                                            cursor += 1;
                                        }
                                    }
                                }
                            }
                        }

                        if !builder.is_empty() {
                            self.vector_by_field_by_scenario
                                .entry(scenario.to_string())
                                .or_insert(HashMap::new())
                                .entry(field.name().to_string())
                                .or_insert_with(|| {
                                    let mut chunk_array = Store::create_chunk_array(field.clone(), self.array_size);
                                    chunk_array.set_array(Arc::new(builder.finish()));
                                    Arc::new(chunk_array)
                                });
                            self.row_mapping_by_field_by_scenario
                                .entry(scenario.to_string())
                                .or_insert(HashMap::new())
                                .entry(field.name().to_string())
                                .or_insert_with(|| row_mapping);
                        }
                    }
                    _ => { panic!("type not supported {}", field.data_type()) }
                }
            }
        }
    }

    fn load_main_scenario(&mut self, scenario: &str, batch: &RecordBatch) {
        let schema = batch.schema();
        for index in 0..batch.columns().len() {
            let col = batch.column(index);

            if index == 0 {
                let mut rc = self.row_count.borrow_mut();
                *rc += col.len() as u64;
            }

            if index as u32 == self.key_indices[0] {
                let arr = col.as_any().downcast_ref::<Int64Array>().unwrap(); // FIXME should not be hardcoded
                let mut r: u64 = 0;
                for b in arr.iter() {
                    self.primary_index.insert(b.unwrap(), r);
                    r += 1;
                }
            }

            let field = schema.field(index);
            match field.data_type().clone() {
                DataType::UInt64 => {
                    let builder = UInt64Builder::new(self.array_size as usize);
                    self.build_base_array::<UInt64Type>(col, scenario, field, builder);
                }
                DataType::UInt32 => {
                    let builder = UInt32Builder::new(self.array_size as usize);
                    self.build_base_array::<UInt32Type>(col, scenario, field, builder);
                }
                DataType::Int64 => {
                    let builder = Int64Builder::new(self.array_size as usize);
                    self.build_base_array::<Int64Type>(col, scenario, field, builder);
                }
                DataType::Float64 => {
                    let builder = Float64Builder::new(self.array_size as usize);
                    self.build_base_array::<Float64Type>(col, scenario, field, builder);
                }
                DataType::Utf8 => {
                    let string_array = col.as_any().downcast_ref::<StringArray>().unwrap();
                    let dic = self.dictionary_provider.dicos
                        .entry(field.name().to_string())
                        .or_insert(Dictionary::new());
                    let mut builder = UInt32Builder::new(string_array.len());
                    for element in string_array {
                        builder.append_value(*dic.map(element.unwrap().to_string())).unwrap();
                    }
                    self.get_chunk_array(scenario, field).set_array(Arc::new(builder.finish()));
                }
                _ => { panic!("type not supported {}", field.data_type()) }
            }
        }
    }

    fn build_base_array<T: ArrowPrimitiveType>(
        &mut self,
        col: &ArrayRef,
        scenario: &str,
        field: &Field,
        mut builder: PrimitiveBuilder<T>) {
        let arr = col.as_any().downcast_ref::<PrimitiveArray<T>>().unwrap();
        for element in arr.iter() {
            builder.append_value(element.unwrap()).unwrap();
        }
        let array = builder.finish();
        self.get_chunk_array(scenario, field).borrow_mut().set_array(Arc::new(array));
    }

    fn build_scenario_array<T: ArrowPrimitiveType>(
        &mut self,
        col: &ArrayRef,
        key_col: &ArrayRef,
        scenario: &str,
        field: &Field,
        mut builder: PrimitiveBuilder<T>) {
        let row_mapping = IntIntMapRowMapping::new();
        let arr = col.as_any().downcast_ref::<PrimitiveArray<T>>().unwrap();
        let key_arr = key_col.as_any().downcast_ref::<Int64Array>().unwrap();

        // Create a block here to borrow vector_by_field_by_scenario as immutable.
        {
            let base_vector = self.vector_by_field_by_scenario.get(MAIN_SCENARIO_NAME).unwrap().get(field.name()).unwrap();
            let array_ref = base_vector.array.borrow();
            let base_arr = array_ref.as_ref().unwrap().as_any().downcast_ref::<PrimitiveArray<T>>().unwrap();

            let mut cursor: u32 = 0;
            for i in 0..arr.len() {
                let key = key_arr.value(i);
                let row = self.primary_index
                    .get(&key)
                    .expect(format!("Cannot find key {} in {} scenario", key, MAIN_SCENARIO_NAME).as_str());
                let row = *row as usize;
                let original_value = base_arr.value(row);
                let value = arr.value(i);
                if original_value != value {
                    builder.append_value(value).unwrap();
                    row_mapping.map(row as u32, cursor);
                    cursor += 1;
                }
            }
        }

        // This block borrow vector_by_field_by_scenario as mutable.
        if !builder.is_empty() {
            self.vector_by_field_by_scenario
                .entry(scenario.to_string())
                .or_insert(HashMap::new())
                .entry(field.name().to_string())
                .or_insert_with(|| {
                    let mut chunk_array = Store::create_chunk_array(field.clone(), self.array_size);
                    chunk_array.set_array(Arc::new(builder.finish()));
                    Arc::new(chunk_array)
                });
            self.row_mapping_by_field_by_scenario
                .entry(scenario.to_string())
                .or_insert(HashMap::new())
                .entry(field.name().to_string())
                .or_insert_with(|| row_mapping);
        }
    }

    fn get_chunk_array(&mut self, scenario: &str, field: &Field) -> &mut Arc<ChunkArray> {
        self.vector_by_field_by_scenario
            .get_mut(scenario)
            .unwrap()
            .get_mut(field.name())
            .unwrap()
    }

    pub fn schema(&self) -> Arc<Schema> {
        Arc::clone(&self.schema)
    }
}
