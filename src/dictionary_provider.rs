
use std::collections::HashMap;
use std::hash::Hash;

#[derive(Debug)]
pub struct DictionaryProvider {
    pub dicos: HashMap<String, Dictionary<String>>
}

impl DictionaryProvider {
    pub fn new() -> DictionaryProvider {
        DictionaryProvider {
            dicos: HashMap::new()
        }
    }
}

#[derive(Debug)]
pub struct Dictionary<T> {
    map: HashMap<T, u32>,
    reverse_map: HashMap<u32, T>,
}

impl<T> Dictionary<T>
where
    T: Eq + Hash,
{
    pub(crate) fn new() -> Dictionary<T> {
        Dictionary {
            map: HashMap::new(),
            reverse_map: HashMap::new(),
        }
    }

    pub fn map(&mut self, value: T) -> &u32 {
        let size = self.map.len();
        let pos = self.map.entry(value).or_insert(size as u32);
        // self.reverse_map.insert(*pos, value);
        pos
    }

    pub fn read(&self, _position: &u32) -> Option<T> {
        None
    }

    pub fn get_position(&self, value: &T) -> Option<&u32> {
        self.map.get(value)
    }
}
