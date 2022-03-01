use std::collections::{HashMap, HashSet};
use std::ops::{BitAndAssign, Range};

use arrow::datatypes::UInt32Type;

use roaring::RoaringBitmap;
use crate::{MAIN_SCENARIO_NAME, SCENARIO_FIELD_NAME, Store};
use crate::chunk_array::ChunkArray;

pub trait RowIterableProvider {
    fn get(&self, scenario: &str) -> RowIterable;
}

pub struct RangeRowIterable {
    pub range: Range<u32>,
}

impl RowIterableProvider for RangeRowIterable {
    fn get(&self, _: &str) -> RowIterable {
        RowIterable::Range { 0: self.range.clone() }
    }
}

pub enum RowIterable {
    RoaringBitmap(RoaringBitmap),
    Range(Range<u32>),
}

impl RowIterable {
    pub fn for_each<F: FnMut(u32) -> ()>(&self, mut f: F) {
        match self {
            RowIterable::RoaringBitmap(bitmap) => {
                bitmap.iter().for_each(f);
            }
            RowIterable::Range(range) => {
                for r in range.start..range.end {
                    f(r);
                }
            }
        }
    }
}

struct RoaringBitmapIntIterableAdapter {
    bitmap: RoaringBitmap,
}

pub struct BitmapRowIterableProvider<'a> {
    accepted_values_by_field: HashMap<String, HashSet<u32>>,
    store: &'a Store,
    initial_iterator: RoaringBitmap,
    fields_with_sim: Vec<String>,
}

impl<'a> RowIterableProvider for BitmapRowIterableProvider<'a> {
    fn get(&self, scenario: &str) -> RowIterable {
        self.create(scenario)
    }
}

impl<'a> BitmapRowIterableProvider<'a> {
    pub fn new(accepted_values_by_field: HashMap<String, HashSet<u32>>, store: &'a Store) -> BitmapRowIterableProvider<'a> {
        if accepted_values_by_field.contains_key(SCENARIO_FIELD_NAME) {
            // The scenarios accepted values should be handled differently. This is a bug...
            panic!("Not expected {:?}", accepted_values_by_field);
        }
        let mut fields_with_sim = Vec::new();
        let bitmap = BitmapRowIterableProvider::create_initial_iterator(&accepted_values_by_field, store, &mut fields_with_sim);
        BitmapRowIterableProvider {
            accepted_values_by_field,
            store,
            initial_iterator: bitmap,
            fields_with_sim,
        }
    }

    pub fn create(&self, scenario: &str) -> RowIterable {
        if !self.fields_with_sim.is_empty() {
            // Clone it because will be modified in-place
            let mut bitmap = self.initial_iterator.clone();
            BitmapRowIterableProvider::apply_conditions(&self.accepted_values_by_field, self.store, &mut bitmap, &self.fields_with_sim, scenario);
            RowIterable::RoaringBitmap(bitmap)
        } else {
            RowIterable::RoaringBitmap(self.initial_iterator.clone())
            // Box::new(RoaringBitmapIntIterableAdapter { bitmap: self.initial_iterator.clone() }) //FIXME can we avoid the cloning here? issue with lifetime if we remove it
        }
    }

    fn create_initial_iterator(accepted_values_by_field: &HashMap<String, HashSet<u32>>, store: &'a Store, fields_with_sim: &mut Vec<String>) -> RoaringBitmap {
        // Keep only the fields that are not simulated
        let mut fields_without_sim = Vec::new();
        for (field, _values) in accepted_values_by_field.iter() {
            let c = store.vector_by_field_by_scenario
                .iter()
                .flat_map(|s| s.1.iter())
                .filter(|e| e.0 == field)
                .count();
            if c > 1 {
                fields_with_sim.push(field.clone());
            } else {
                fields_without_sim.push(field.clone());
            }
        }

        // Lexical sort to have a deterministic order
        fields_without_sim.sort();
        fields_with_sim.sort();

        let mut res = Vec::new();
        fields_without_sim.iter().for_each(|f| res.push(store.vector_by_field_by_scenario.get(MAIN_SCENARIO_NAME).unwrap().get(f).unwrap()));

        let first_field = fields_without_sim.remove(0);
        let mut bitmap = BitmapRowIterableProvider::initialize_bitmap(
            store,
            accepted_values_by_field.get(first_field.as_str()).unwrap(),
            store.get_scenario_chunk_array(MAIN_SCENARIO_NAME, first_field.as_str()));
        BitmapRowIterableProvider::apply_conditions(
            accepted_values_by_field,
            store,
            &mut bitmap,
            &fields_without_sim,
            MAIN_SCENARIO_NAME);

        bitmap
    }

    fn initialize_bitmap(store: &'a Store, accepted_values: &HashSet<u32>, vector: &ChunkArray) -> RoaringBitmap {
        let mut matching_rows = RoaringBitmap::new();
        for row in 0..*store.row_count.borrow() {
            if accepted_values.contains(&vector.read::<UInt32Type>(row as u32)) {
                matching_rows.insert(row as u32);
            }
        }
        matching_rows
    }

    fn apply_conditions(accepted_values_by_field: &HashMap<String, HashSet<u32>>, store: &'a Store, bitmap: &mut RoaringBitmap, fields: &Vec<String>, scenario: &str) {
        for field in fields {
            let mut tmp = RoaringBitmap::new();
            let values = accepted_values_by_field.get(field.as_str()).unwrap();
            let column = store.get_scenario_chunk_array(scenario, field.as_str());
            for row in bitmap.iter() {
                if values.contains(&column.read::<UInt32Type>(row as u32)) {
                    tmp.insert(row as u32);
                }
            }
            bitmap.bitand_assign(tmp);
        }
    }
}