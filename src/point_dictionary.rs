use std::collections::HashMap;
use crate::dictionary_provider::Dictionary;


#[derive(Debug)]
pub struct PointDictionary {
    dic: Dictionary<Vec<u32>>,
    point_length: u32,
}

impl PointDictionary {
    pub fn new(point_length: u32) -> PointDictionary {
        PointDictionary {
            dic: Dictionary::new(),
            point_length,
        }
    }

    pub fn map(&mut self, point: &[u32]) -> &u32 {
        self.dic.map(Vec::from(point))
    }

    pub fn read(&self, position: &u32) -> Option<&Vec<u32>> {
        self.dic.read(position)
    }

    pub fn get_position(&self, point: &[u32]) -> Option<&u32> {
        self.dic.get_position(&Vec::from(point))
    }

    pub fn size(&self) -> usize {
        self.dic.size()
    }

    pub fn len(&self) -> u32 {
        self.point_length
    }
}
