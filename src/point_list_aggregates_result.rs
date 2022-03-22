use std::any::Any;
use std::collections::HashMap;
use std::fmt;
use std::fmt::Debug;
use std::hash::Hash;
use std::sync::Arc;
use arrow::array;

use arrow::array::{Array, Float64Array, UInt32Array};
use arrow::datatypes::{DataType, Int16Type, Int32Type, Int64Type, Int8Type, IntervalUnit, TimeUnit, UInt16Type, UInt32Type, UInt64Type, UInt8Type};
use arrow::error::ArrowError;
use arrow::util::display::make_string_from_decimal;
use comfy_table::{Table, Cell};

use crate::dictionary_provider::Dictionary;
use crate::aggregator::Aggregator;
use crate::{make_string, assert_row_value};
use crate::point_dictionary::PointDictionary;

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

    pub fn assert_aggregate<K: 'static + std::fmt::Debug>(&self, coordinates: Vec<&str>, expected_value: K) { // FIXME why 'static is needed?
        let mut buffer = Vec::with_capacity(self.point_names.len());
        for i in 0..coordinates.len() {
            let s = coordinates[i];
            let pos = self.dictionaries[i]
                .get_position(&String::from(s))
                .expect(format!("Cannot find position of {}", s).as_str())
                .clone();
            buffer.push(pos);
        }
        let position = self.point_dictionary.get_position(&buffer[..]);
        match position {
            None => {}
            Some(row) => {
                for i in 0..self.aggregators.len() {
                    let array = self.aggregators[i].get_destination();
                    let r = *row as usize;
                    match array.data_type() {
                        DataType::UInt32 => assert_row_value!(UInt32Array, u32, array, r, expected_value),
                        DataType::Float64 => assert_row_value!(Float64Array, f64, array, r, expected_value),
                        _ => panic!("assert not implemented for {:?} type", array.data_type()),
                    }
                }
            }
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

#[macro_export]
macro_rules! assert_row_value {
     ($data_type: ty, $type: ty, $column: ident, $row: ident, $expected_row_value: ident) => {{
        let array = $column.as_any().downcast_ref::<$data_type>().unwrap();
        let any: &dyn Any = &$expected_row_value;
        assert_eq!(&array.value($row), any.downcast_ref::<$type>().unwrap());
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