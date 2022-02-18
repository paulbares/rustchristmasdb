use std::cell::RefCell;
use std::collections::HashMap;
use std::fmt::Formatter;
use arrow::datatypes::Field;
use crate::chunk_array::ChunkArray;
use crate::row_mapping::{IdentityMapping, RowMapping};

const MAIN_SCENARIO_NAME: &str = "base";

#[derive(Debug)]
pub struct Store {
    fields: Vec<Field>,
    key_indices: Vec<u32>,
    array_size: u32,
    vector_by_field_by_scenario: RefCell<HashMap<String, HashMap<String, ChunkArray>>>,
    row_mapping_by_field_by_scenario: RefCell<HashMap<String, HashMap<String, Box<dyn RowMapping>>>>,
}

impl Store {
    pub fn new(fields: Vec<Field>, key_indices: Vec<u32>, array_size: u32) -> Store {
        let mut vector_by_field_by_scenario = HashMap::new();
        let mut row_mapping_by_field_by_scenario: HashMap<String, HashMap<String, Box<dyn RowMapping>>> = HashMap::new();
        fields.iter().for_each(|f| {
            vector_by_field_by_scenario
                .entry(MAIN_SCENARIO_NAME.to_string())
                .or_insert(HashMap::new())
                .entry(f.name().clone())
                .or_insert_with(|| Store::create_chunk_array(f));
            row_mapping_by_field_by_scenario
                .entry(MAIN_SCENARIO_NAME.to_string())
                .or_insert(HashMap::new())
                .entry(f.name().clone())
                .or_insert_with(|| Box::new(IdentityMapping::new()));
        });

        Store {
            fields,
            key_indices,
            array_size,
            vector_by_field_by_scenario: RefCell::new(vector_by_field_by_scenario),
            row_mapping_by_field_by_scenario: RefCell::new(row_mapping_by_field_by_scenario),
        }
    }

    fn create_chunk_array(field: &Field) -> ChunkArray {
        ChunkArray::new()
    }

    pub fn load<T>(&self, scenario: &str, tuples: Vec<T>) {
        self.vector_by_field_by_scenario.borrow_mut().insert("zob".to_string(), HashMap::new());
    }
}