use std::any::Any;
use std::fmt;
use std::rc::Rc;
use arrow::array::Array;
use crate::chunk_array::ChunkArray;
use crate::dictionary_provider::Dictionary;
use crate::PointDictionary;

pub struct PointListAggregateResult<'a> {
    point_dictionary: PointDictionary,
    point_names: Vec<String>,
    aggregates: Vec<&'a dyn Array>,
    aggregate_names: Vec<String>,
    dictionaries: Vec<&'a Dictionary<String>>,
}

impl<'a> PointListAggregateResult<'a> {
    pub fn new(point_dictionary: PointDictionary,
               point_names: Vec<String>,
               dictionaries: Vec<&'a Dictionary<String>>,
               aggregates: Vec<&'a dyn Array>,
               aggregate_names: Vec<String>) -> PointListAggregateResult<'a> {
        PointListAggregateResult {
            point_dictionary,
            point_names,
            dictionaries,
            aggregates,
            aggregate_names,
        }
    }

    fn size(&self) -> usize {
        self.point_dictionary.size()
    }
}

impl fmt::Display for PointListAggregateResult<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_list()
            .entries(self.point_names.iter())
            .entries(self.aggregate_names.iter())
            .finish();
        write!(f, "({:?}, {:?}, {})", self.point_dictionary, self.aggregates, self.size())
    }
}