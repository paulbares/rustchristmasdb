use std::collections::HashMap;
use std::hash::Hash;

pub struct PointDictionary<T> {
    dic: HashMap<T, u32>,
}

impl<T: std::cmp::Eq + Hash> PointDictionary<T> {
    pub fn new() -> PointDictionary<T> {
        PointDictionary {
            dic: HashMap::new(),
        }
    }

    pub fn map(&mut self, point: T) -> u32 {
        let len = self.dic.len();
        self.dic
            .entry(point)
            .or_insert(len as u32);
        len as u32
    }
}
