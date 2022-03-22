

use indexmap::IndexMap;

pub struct Query<'a> {
    pub coordinates: IndexMap<String, Option<Vec<String>>>, // Use IndexMap to preserve the order.
    pub measures: Vec<AggregatedMeasure<'a>>,
}

impl<'a> Query<'a> {
    pub fn new() -> Query<'a> {
        Query { coordinates: IndexMap::new(), measures: Vec::new() }
    }

    pub fn add_wildcard_coordinate(&mut self, field: &str) -> &mut Query<'a> {
        self.coordinates.insert(field.to_string(), None);
        self
    }

    pub fn add_coordinates(&mut self, field: &str, coordinates: Vec<&str>) -> &mut Query<'a> {
        let v: Vec<String> = coordinates.iter().map(|c| c.to_string()).collect();
        self.coordinates.insert(field.to_string(), Some(v));
        self
    }

    pub fn add_aggregated_measure(&mut self, field: &'a str, agg: &'a str) -> &mut Query<'a> {
        self.measures.push(AggregatedMeasure::new(field, agg));
        self
    }
}

pub struct AggregatedMeasure<'a> {
    pub field: &'a str,
    pub aggregation_function: &'a str,
}

impl<'a> AggregatedMeasure<'a> {
    pub fn new(field: &'a str, aggregation_function: &'a str) -> Self {
        AggregatedMeasure { field, aggregation_function }
    }

    pub fn alias(&self) -> String {
        format!("{}({})", self.aggregation_function, self.field)
    }
}