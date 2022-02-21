use std::collections::HashMap;
use std::hash::Hash;

pub struct DictionaryProvider {
    // dicts: HashMap<String, Dictionary<T>>
}

pub struct Dictionary<T> {
    map: HashMap<T, u32>,
    reverse_map: HashMap<u32, T>,
}

impl<T> Dictionary<T>
where
    T: Eq + Hash,
{
    fn new() -> Dictionary<T> {
        Dictionary {
            map: HashMap::new(),
            reverse_map: HashMap::new(),
        }
    }

    fn map(&mut self, value: T) -> u32 {
        let size = self.map.len();
        let pos = self.map.entry(value).or_insert(size as u32);
        // self.reverse_map.insert(*pos, value);
        (size + 1) as u32
    }

    fn read(&self, position: &u32) -> Option<T> {
        None
    }

    fn get_position(&self, value: &T) -> Option<&u32> {
        self.map.get(value)
    }
}
