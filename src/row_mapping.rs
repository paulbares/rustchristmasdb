use std::cell::RefCell;
use std::collections::HashMap;
use std::fmt;
use std::fmt::Formatter;
use std::sync::Arc;

pub trait RowMapping {
    fn map(&self, row: u32, target_row: u32);

    fn get(&self, row: &u32) -> Option<u32>;

    fn debug(&self) -> String;
}

impl std::fmt::Debug for dyn RowMapping {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.debug())
    }
}

// pub const IDENTITY_MAPPING: IdentityMapping = IdentityMapping {};

#[derive(Debug)]
pub struct IdentityMapping {}

impl RowMapping for IdentityMapping {
    fn map(&self, _row: u32, _target_row: u32) {
        // noop
    }

    fn get(&self, row: &u32) -> Option<u32> {
        Some(*row)
    }

    fn debug(&self) -> String {
        String::from("identity_mapping")
    }
}

impl IdentityMapping {
    pub fn new() -> IdentityMapping {
        IdentityMapping {}
    }
}

#[derive(Debug)]
pub struct IntIntMapRowMapping {
    mapping: RefCell<HashMap<u32, u32>>,
}

impl IntIntMapRowMapping {
    pub fn new() -> Arc<dyn RowMapping> {
        Arc::new(IntIntMapRowMapping {
            mapping: RefCell::new(HashMap::new()),
        })
    }
}

impl RowMapping for IntIntMapRowMapping {
    fn map(&self, row: u32, target_row: u32) {
        self.mapping.borrow_mut().insert(row, target_row);
    }

    fn get(&self, row: &u32) -> Option<u32> {
        self.mapping.borrow().get(row).cloned()
    }

    fn debug(&self) -> String {
        format!("int_int_row_mapping: {:?}", *self.mapping.borrow())
    }
}

impl fmt::Display for IntIntMapRowMapping {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "Mapping {:?}", *self.mapping.borrow())
    }
}