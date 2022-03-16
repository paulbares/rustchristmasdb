use std::borrow::Borrow;
use std::cell::RefCell;
use std::sync::Arc;
use arrow::array::{Array, ArrayRef, PrimitiveArray};

use arrow::datatypes::{ArrowPrimitiveType, DataType, Field};
use crate::row_mapping::RowMapping;

#[derive(Debug)]
pub enum ChunkArrayReader {
    BaseReader { base_array: Arc<ChunkArray> },
    ScenarioReader {
        base_array: Arc<ChunkArray>,
        scenario: String,
        scenario_array: Arc<ChunkArray>,
        row_mapping: Arc<dyn RowMapping>,
    },
}

impl ChunkArrayReader {
    pub fn read<T: ArrowPrimitiveType>(&self, row: u32) -> T::Native {
        match self {
            ChunkArrayReader::BaseReader { base_array } => {
                let array_ref = base_array.array.borrow();
                let array = array_ref.as_ref().unwrap().as_any().downcast_ref::<PrimitiveArray<T>>().unwrap();
                unsafe { array.value_unchecked(row as usize) }
            }
            ChunkArrayReader::ScenarioReader { base_array, scenario_array, scenario, row_mapping } => {
                let scenario_row = row_mapping.get(&row);
                match scenario_row {
                    None => {
                        let array_ref = base_array.array.borrow();
                        let array = array_ref.as_ref().unwrap().as_any().downcast_ref::<PrimitiveArray<T>>().unwrap();
                        unsafe { array.value_unchecked(row as usize) }
                    }
                    Some(sr) => {
                        let array_ref = scenario_array.array.borrow();
                        let array = array_ref.as_ref().unwrap().as_any().downcast_ref::<PrimitiveArray<T>>().unwrap();
                        unsafe { array.value_unchecked(sr as usize) }
                    }
                }
            }
        }
    }

    pub fn data_type(&self) -> &DataType {
        match self {
            ChunkArrayReader::BaseReader { base_array } => {
                base_array.field.data_type()
            }
            ChunkArrayReader::ScenarioReader { base_array, scenario_array, scenario, row_mapping } => {
                base_array.field.data_type()
            }
        }
    }
}

#[derive(Debug)]
pub struct ChunkArray {
    pub field: Field,
    pub array: RefCell<Option<ArrayRef>>,
}

impl ChunkArray {
    pub fn new(field: Field, size: u32) -> ChunkArray {
        if !ChunkArray::is_power_of_two(size) {
            panic!("{} not a power of 2", size);
        }

        ChunkArray {
            field,
            array: RefCell::new(None),
        }
    }

    pub fn read<T: ArrowPrimitiveType>(&self, row: u32) -> T::Native {
        let array_ref = self.array.borrow();
        let array = array_ref.as_ref().unwrap().as_any().downcast_ref::<PrimitiveArray<T>>().unwrap();

        unsafe { array.value_unchecked(row as usize) }
    }

    pub fn set_array(&self, array: Arc<dyn Array>) {
        self.array.borrow_mut().replace(ArrayRef::from(array));
    }

    fn is_power_of_two(number: u32) -> bool {
        number > 0 && ((number & (number - 1)) == 0)
    }
}
