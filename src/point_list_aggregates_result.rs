use std::any::Any;
use std::fmt;
use std::rc::Rc;
use arrow::array::Array;
use comfy_table::{Table, Cell};
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