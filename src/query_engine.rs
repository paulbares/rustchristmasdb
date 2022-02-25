use std::borrow::BorrowMut;
use std::collections::{HashMap, HashSet};
use std::ops::Index;
use arrow::array::Array;
use arrow::datatypes::UInt32Type;
use datastore::MAIN_SCENARIO_NAME;
use crate::{Aggregator, AggregatorFactory, datastore, PointDictionary, PointListAggregateResult, Query, SCENARIO_FIELD_NAME, Store};
use crate::bitmap_row_iterable_provider::BitmapRowIterableProvider;
use crate::row_iterable_provider::RowIterableProviderFactory;

pub struct QueryEngine<'a> {
    store: &'a Store,
}

impl<'a> QueryEngine<'a> {
    pub fn new(store: &'a Store) -> QueryEngine<'a> {
        QueryEngine { store }
    }

    pub fn execute(&self, query: &'a Query) -> () {
        let accepted_values_by_field = self.compute_accepted_values(query);
        let queried_scenarios = self.compute_queried_scenarios(query);
        let mut aggregators_by_scenario = self.compute_aggregators(query, queried_scenarios.clone());

        let point_size = query.coordinates.len();
        let mut point_dictionary = PointDictionary::new(point_size as u32);
        let point_names: Vec<String> = query.coordinates.keys().map(|k| k.to_string()).collect();
        let scenario_index = point_names.iter().position(|r| *r == SCENARIO_FIELD_NAME).unwrap_or(usize::MAX);
        let provider = RowIterableProviderFactory::create(self.store, accepted_values_by_field);
        for i in queried_scenarios.iter() {
            let dictionary = self.store.get_dictionary(SCENARIO_FIELD_NAME);
            let scenario = dictionary.read(&i).unwrap();

            let mut columns = Vec::with_capacity(point_size);
            columns.resize(point_size, None);
            for point_index in 0..point_size {
                if point_index != scenario_index {
                    columns[point_index] = Some(self.store.get_scenario_chunk_array(scenario.as_str(), point_names[point_index].as_str()));
                }
            }
            let aggregators = aggregators_by_scenario.get_mut(scenario).unwrap();

            provider.get(scenario.as_str()).for_each(|row| {
                let mut point: Vec<u32> = Vec::with_capacity(point_size);
                point.resize(point_size, 0);
                for point_index in 0..point_size {
                    if point_index != scenario_index {
                        point[point_index] = columns[point_index].unwrap().read::<UInt32Type>(row);
                    } else {
                        point[point_index] = *i;
                    }
                }

                let destination_row = point_dictionary.map(point.as_slice());
                // And then aggregate
                let mut check = false;
                for aggregator in aggregators.into_iter() {
                    if !check {
                        // aggregator.get_destination().ensure_capacity(); // TODO to impl.
                        check = true;
                    }
                    aggregator.as_mut().aggregate(row, destination_row);
                }
            });
        }

        aggregators_by_scenario.iter_mut()
            .flat_map(|(k, v)| v.into_iter())
            .for_each(|a| a.as_mut().finish());

        println!("{:?}", scenario_index);

        let dictionaries = point_names.iter().map(|name| self.store.dictionary_provider.dicos.get(name).unwrap()).collect();
        PointListAggregateResult::new(point_dictionary,
                                      point_names,
                                      dictionaries,
                                      Vec::new(),
                                      Vec::new());
    }

    fn compute_accepted_values(&self, query: &Query) -> HashMap<String, HashSet<u32>> {
        let mut accepted_values_by_field: HashMap<String, HashSet<u32>> = HashMap::new();
        query.coordinates.iter().for_each(|(field, values)| {
            if *field == SCENARIO_FIELD_NAME {
                return;
            }

            if let Some(coords) = values {
                let dictionary = self.store.dictionary_provider.dicos
                    .get(field)
                    .expect(format!("cannot find dic. for field {}", field).as_str());
                for coord in coords {
                    if let Some(position) = dictionary.get_position(coord) {
                        accepted_values_by_field.entry(field.to_string())
                            .or_insert(HashSet::new())
                            .insert(*position);
                    }
                }
            }
        });
        accepted_values_by_field
    }

    fn compute_queried_scenarios(&self, query: &Query) -> Vec<u32> {
        let mut values: Option<Vec<String>> = None;
        if query.coordinates.contains_key(SCENARIO_FIELD_NAME) {
            // This condition handles wildcard coordinates.
            if let None = query.coordinates.get(SCENARIO_FIELD_NAME).unwrap() {
                values = Some(self.store.vector_by_field_by_scenario.keys().map(|k| k.to_string()).collect());
            }
        } else {
            values = Some(vec![MAIN_SCENARIO_NAME.to_string()]);
        }

        let mut scenarios: Vec<u32> = Vec::new();
        let dictionary = self.store.get_dictionary(SCENARIO_FIELD_NAME);
        if let Some(vv) = values {
            for value in vv {
                if let Some(position) = dictionary.get_position(&value) {
                    scenarios.push(*position)
                }
            }
        }
        scenarios
    }

    fn compute_aggregators(&self, query: &Query, queried_scenarios: Vec<u32>) -> HashMap<String, Vec<Box<dyn Aggregator>>> {
        let mut aggregators_by_scenario = HashMap::new();
        let factory = AggregatorFactory::new();
        let dictionary = self.store.get_dictionary(SCENARIO_FIELD_NAME);
        for (index, s) in queried_scenarios.iter().enumerate() {
            let scenario = dictionary.read(s).unwrap();
            let mut aggregators: Vec<Box<dyn Aggregator>> = Vec::new();
            if index == 0 {
                query.measures.iter().for_each(|measure| {
                    let source = self.store.get_scenario_chunk_array(scenario, &measure.field.to_string());
                    let aggregator = factory.create(
                        source,
                        measure.aggregation_function,
                        "");
                    aggregators.push(aggregator);
                    // aggregates.push(aggregator.get_destination()); FIXME
                });
            } else {
                // Here, we take the destination column created earlier.
                for i in 0..query.measures.len() {
                    let measure = &query.measures[i];
                    let source = self.store.get_scenario_chunk_array(scenario, &measure.field.to_string());
                    let aggregator = factory.create_with_destination(
                        source,
                        // aggregates.get(i),
                        measure.aggregation_function);
                    // aggregators.push(aggregator);
                }
            }
            aggregators_by_scenario.insert(scenario.to_string(), aggregators);
        }
        aggregators_by_scenario
    }
}
