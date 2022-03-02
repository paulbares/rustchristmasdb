use std::collections::HashMap;


#[derive(Debug)]
pub struct PointDictionary {
    dic: HashMap<Vec<u32>, u32>,
    reverse_dic: HashMap<u32, Vec<u32>>,
    point_length: u32,
}

impl PointDictionary {
    pub fn new(point_length: u32) -> PointDictionary {
        PointDictionary {
            dic: HashMap::new(),
            reverse_dic: HashMap::new(),
            point_length
        }
    }

    pub fn map(&mut self, point: &[u32]) -> &u32 {
        let len = self.dic.len();
        let pos = *self.dic
            .entry(Vec::from(point))
            .or_insert(len as u32);

        return &pos
    }

    // pub fn read(&self, row: u32) -> &[u32] {
    //     self.dic.ge
    // }

    pub fn size(&self) -> usize {
        self.dic.len()
    }

    pub fn len(&self) -> u32 {
        self.point_length
    }
}
