use std::borrow::Borrow;
use arrow::array::{Array, ArrayRef, PrimitiveArray};

use arrow::datatypes::{ArrowPrimitiveType, Field};
use crate::row_mapping::RowMapping;

#[derive(Debug)]
pub enum ChunkArrayReader<'a> {
    BaseReader { base_array: &'a ChunkArray },
    ScenarioReader {
        base_array: &'a ChunkArray,
        scenario_array: &'a ChunkArray,
        scenario: String,
        row_mapping: &'a Box<dyn RowMapping>,
    },
}

impl<'a> ChunkArrayReader<'a> {
    pub fn read<T: ArrowPrimitiveType>(&self, row: u32) -> T::Native {
        match self {
            ChunkArrayReader::BaseReader { base_array } => {
                let array = base_array.array.as_ref().unwrap().as_any().downcast_ref::<PrimitiveArray<T>>().unwrap();
                unsafe { array.value_unchecked(row as usize) }
            }
            ChunkArrayReader::ScenarioReader { base_array, scenario_array, scenario, row_mapping } => {
                let scenario_row = row_mapping.get(&row);
                match scenario_row {
                    None => {
                        let array = base_array.array.as_ref().unwrap().as_any().downcast_ref::<PrimitiveArray<T>>().unwrap();
                        unsafe { array.value_unchecked(row as usize) }
                    }
                    Some(sr) => {
                        let array = scenario_array.array.as_ref().unwrap().as_any().downcast_ref::<PrimitiveArray<T>>().unwrap();
                        unsafe { array.value_unchecked(sr as usize) }
                    }
                }
            }
        }
    }
}

#[derive(Debug)]
pub struct ChunkArray {
    pub field: Field,
    pub array: Option<ArrayRef>,
}

impl ChunkArray {
    pub fn new(field: Field, size: u32) -> ChunkArray {
        if !ChunkArray::is_power_of_two(size) {
            panic!("{} not a power of 2", size);
        }

        ChunkArray {
            field,
            array: None,
        }
    }

    pub fn read<T: ArrowPrimitiveType>(&self, row: u32) -> T::Native {
        let array = self.array.as_ref().unwrap().as_any().downcast_ref::<PrimitiveArray<T>>().unwrap();

        unsafe { array.value_unchecked(row as usize) }
    }

    pub fn set_array(&mut self, array: Box<dyn Array>) {
        self.array = Some(ArrayRef::from(array));
    }

    fn is_power_of_two(number: u32) -> bool {
        number > 0 && ((number & (number - 1)) == 0)
    }
}
