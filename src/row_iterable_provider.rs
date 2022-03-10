use std::collections::{HashMap, HashSet};
use std::ops::Range;
use crate::bitmap_row_iterable_provider::{BitmapRowIterableProvider, RangeRowIterable, RowIterableProvider};
use crate::datastore::Store;

pub struct RowIterableProviderFactory;

impl RowIterableProviderFactory {
    pub fn create<'a>(store: &'a Store, accepted_values_by_field: HashMap<String, HashSet<u32>>) -> Box<dyn RowIterableProvider + 'a> {
        if accepted_values_by_field.is_empty() {
            Box::new(RangeRowIterable {
                range: Range {
                    start: 0,
                    end: *store.row_count.borrow() as u32,
                }
            })
        } else {
            Box::new(BitmapRowIterableProvider::new(accepted_values_by_field, store))
        }
    }
}