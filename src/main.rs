extern crate core;



use std::sync::Arc;

use arrow::array::{Float64Array, StringArray, UInt32Array, UInt64Array};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::{record_batch::RecordBatch, util::pretty::print_batches};
use crate::aggregator::{Aggregator, AggregatorFactory};

use crate::datastore::{CHUNK_DEFAULT_SIZE, MAIN_SCENARIO_NAME, SCENARIO_FIELD_NAME, Store};

use crate::point_dictionary::PointDictionary;
use crate::point_list_aggregates_result::PointListAggregateResult;
use crate::query::Query;
use crate::query_engine::QueryEngine;

mod chunk_array;
mod datastore;
mod dictionary_provider;
mod row_mapping;
mod point_dictionary;
mod aggregator;
mod point_list_aggregates_result;
mod query;
mod query_engine;
mod bitmap_row_iterable_provider;
mod row_iterable_provider;

fn main() {
    let schema = Schema::new(vec![
        Field::new("id", DataType::UInt64, false),
        Field::new("product", DataType::Utf8, false),
        Field::new("category", DataType::Utf8, false),
        Field::new("price", DataType::Float64, false),
        Field::new("quantity", DataType::UInt32, false),
    ]);

    let schema_ref = Arc::new(schema);
    let mut store = Store::new(schema_ref, vec![0], CHUNK_DEFAULT_SIZE as u32);

    let main_batch = create_main_batch(&store);
    let s1_batch = create_s1_batch(&store);

    store.load(MAIN_SCENARIO_NAME, &main_batch);
    store.load("s1", &s1_batch);

    println!("Datastore: {:?}", store);
    print_batches(&[main_batch]).unwrap();

    let mut query = Query::new();
    let query = query
        .add_wildcard_coordinate(SCENARIO_FIELD_NAME)
        .add_coordinates("category", vec!["condiment", "milk"])
        .add_aggregated_measure("price", "sum")
        .add_aggregated_measure("quantity", "sum");

    let qe = QueryEngine::new(&store);
    let result = qe.execute(query);
    println!("{}", result);
    // PointListAggregateResult result = queryEngine.execute(query);

    // let col = price_aggregator.as_any().downcast_ref::<SumFloat64Aggregator>().unwrap();
    // println!("{:?}", col.get_destination());
    // let col = quantity_aggregator.as_any().downcast_ref::<SumUIntAggregator>().unwrap();
    // println!("{:?}", col.get_destination());
}

fn create_main_batch(store: &Store) -> RecordBatch {
    let id_array = UInt64Array::from(vec![0, 1, 2, 3, 4]);
    let quantity_array = UInt32Array::from(vec![5, 3, 4, 1, 4]);
    let product_array =
        StringArray::from(vec!["syrup", "tofu", "mozzarella", "syrup", "tofu"]);
    let category_array =
        StringArray::from(vec!["condiment", "milk", "milk", "condiment", "milk"]);
    let price_array = Float64Array::from(vec![2f64, 8f64, 4f64, 2f64, 8f64]);

    RecordBatch::try_new(
        store.schema(),
        vec![
            Arc::new(id_array),
            Arc::new(product_array),
            Arc::new(category_array),
            Arc::new(price_array),
            Arc::new(quantity_array),
        ],
    ).unwrap()
}

fn create_s1_batch(store: &Store) -> RecordBatch {
    let id_array = UInt64Array::from(vec![0, 1, 2, 3, 4]);
    let quantity_array = UInt32Array::from(vec![5, 4, 4, 1, 4]);
    let product_array =
        StringArray::from(vec!["syrup", "tofu", "mozzarella", "syrup", "tofu"]);
    let category_array =
        StringArray::from(vec!["condiment", "milk", "milk", "condiment", "milk"]);
    let price_array = Float64Array::from(vec![1f64, 8f64, 4f64, 2f64, 8f64]);

    RecordBatch::try_new(
        store.schema(),
        vec![
            Arc::new(id_array),
            Arc::new(product_array),
            Arc::new(category_array),
            Arc::new(price_array),
            Arc::new(quantity_array),
        ],
    ).unwrap()
}
