use std::collections::HashMap;
use std::fmt;

use arrow::array::Array;
use comfy_table::{Table, Cell};

use crate::dictionary_provider::Dictionary;
use crate::{Aggregator, PointDictionary};

pub struct PointListAggregateResult<'a> {
    point_dictionary: PointDictionary,
    point_names: Vec<String>,
    aggregators: Vec<Box<dyn Aggregator>>,
    aggregate_names: Vec<String>,
    dictionaries: Vec<&'a Dictionary<String>>,
}

impl<'a> PointListAggregateResult<'a> {
    pub fn new(point_dictionary: PointDictionary,
               point_names: Vec<String>,
               dictionaries: Vec<&'a Dictionary<String>>,
               aggregators_by_scenario: HashMap<String, Vec<Box<dyn Aggregator>>>) -> PointListAggregateResult<'a> {
        let mut aggregators_vec = Vec::new();
        for (_, aggregators) in aggregators_by_scenario.into_iter() {
            for aggregator in aggregators.into_iter() {
                aggregators_vec.push(aggregator);
            }
            break;
        }

        let aggregate_names = aggregators_vec.iter()
            .map(|a| a.get_field().name().to_string())
            .collect();

        PointListAggregateResult {
            point_dictionary,
            point_names,
            dictionaries,
            aggregators: aggregators_vec,
            aggregate_names,
        }
    }

    fn size(&self) -> usize {
        self.point_dictionary.size()
    }

    ///! Convert a series of record batches into a table
    fn create_table(&self) -> Table {
        let mut table = Table::new();
        table.load_preset("||--+-++|    ++++++");

        if self.size() == 0 {
            return table;
        }

        let mut header = Vec::new();
        for field in &self.point_names {
            header.push(Cell::new(field));
        }
        for field in &self.aggregate_names {
            header.push(Cell::new(field));
        }
        table.set_header(header);

        for row in 0..self.point_dictionary.size() {
            // let mut cells = Vec::new();
            for p in 0..self.point_dictionary.len() {
                // self.point_dictionary.read
            }
        }

        // for batch in results {
        //     for row in 0..batch.num_rows() {
        //         let mut cells = Vec::new();
        //         for col in 0..batch.num_columns() {
        //             let column = batch.column(col);
        //             // cells.push(Cell::new(&array_value_to_string(column, row)?));
        //             cells.push(Cell::new("&array_value_to_string(column, row)?"));
        //         }
        //         table.add_row(cells);
        //     }
        // }

        table
    }
}

impl fmt::Display for PointListAggregateResult<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        // f.debug_list()
        //     .entries(self.point_names.iter())
        //     .entries(self.aggregate_names.iter())
        //     .finish();
        write!(f, "{}", self.create_table())
        // write!(f, "({:?}, {:?}, {})", self.point_dictionary, self.aggregates, self.size())
    }
}