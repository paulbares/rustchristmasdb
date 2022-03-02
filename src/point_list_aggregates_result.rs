use std::collections::HashMap;
use std::fmt;
use std::sync::Arc;
use arrow::array;

use arrow::array::Array;
use arrow::datatypes::{DataType, Int16Type, Int32Type, Int64Type, Int8Type, IntervalUnit, TimeUnit, UInt16Type, UInt32Type, UInt64Type, UInt8Type};
use arrow::error::ArrowError;
use arrow::util::display::make_string_from_decimal;
use comfy_table::{Table, Cell};

use crate::dictionary_provider::Dictionary;
use crate::{Aggregator, PointDictionary};
use crate::make_string;

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
            let mut cells = Vec::new();
            let point = self.point_dictionary.read(&(row as u32)).unwrap();
            for p in 0..point.len() {
                let o = self.dictionaries[p].read(&point[p]).unwrap();
                cells.push(Cell::new(o));
            }

            for aggregator in self.aggregators.iter() {
                let array = aggregator.get_destination();
                cells.push(Cell::new(array_value_to_string(array, row).unwrap()));
            }
            table.add_row(cells);
        }

        table
    }
}

pub fn array_value_to_string(column: &dyn Array, row: usize) -> Result<String, ArrowError> {
    if column.is_null(row) {
        return Ok("".to_string());
    }
    match column.data_type() {
        DataType::Utf8 => make_string!(array::StringArray, column, row),
        DataType::Boolean => make_string!(array::BooleanArray, column, row),
        DataType::Int8 => make_string!(array::Int8Array, column, row),
        DataType::Int16 => make_string!(array::Int16Array, column, row),
        DataType::Int32 => make_string!(array::Int32Array, column, row),
        DataType::Int64 => make_string!(array::Int64Array, column, row),
        DataType::UInt8 => make_string!(array::UInt8Array, column, row),
        DataType::UInt16 => make_string!(array::UInt16Array, column, row),
        DataType::UInt32 => make_string!(array::UInt32Array, column, row),
        DataType::UInt64 => make_string!(array::UInt64Array, column, row),
        DataType::Float16 => make_string!(array::Float16Array, column, row),
        DataType::Float32 => make_string!(array::Float32Array, column, row),
        DataType::Float64 => make_string!(array::Float64Array, column, row),
        _ => Err(ArrowError::InvalidArgumentError(format!(
            "Pretty printing not implemented for {:?} type",
            column.data_type()
        ))),
    }
}

#[macro_export]
macro_rules! make_string {
    ($array_type:ty, $column: ident, $row: ident) => {{
        let array = $column.as_any().downcast_ref::<$array_type>().unwrap();

        let s = if array.is_null($row) {
            "".to_string()
        } else {
            array.value($row).to_string()
        };

        Ok(s)
    }};
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