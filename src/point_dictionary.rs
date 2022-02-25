use std::collections::HashMap;
use std::hash::Hash;

#[derive(Debug)]
pub struct PointDictionary {
    dic: HashMap<Vec<u32>, u32>,
    point_length: u32,
}

impl PointDictionary {
    pub fn new(point_length: u32) -> PointDictionary {
        PointDictionary {
            dic: HashMap::new(),
            point_length
        }
    }

    pub fn map(&mut self, point: &[u32]) -> u32 {
        let len = self.dic.len();
        *self.dic
            .entry(Vec::from(point))
            .or_insert(len as u32)
    }

    pub fn size(&self) -> usize {
        self.dic.len()
    }
}
