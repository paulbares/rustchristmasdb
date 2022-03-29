use std::sync::Arc;
use arrow::array::{Float64Array, Int64Array, StringArray, UInt32Array};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;

use rustchristmasdb::datastore::{CHUNK_DEFAULT_SIZE, MAIN_SCENARIO_NAME, SCENARIO_FIELD_NAME, Store};
use rustchristmasdb::query::Query;
use rustchristmasdb::query_engine::QueryEngine;

#[test]
fn test_wildcard_without_scenario_in_the_query() {
    let store = build_and_load();

    let mut query = Query::new();
    let query = query
        .add_wildcard_coordinate("product")
        .add_aggregated_measure("price", "sum");

    let qe = QueryEngine::new(&store);
    let result = qe.execute(query);
    result.assert_aggregate(Vec::from(["syrup"]), 2f64);
    result.assert_aggregate(Vec::from(["tofu"]), 8f64);
    result.assert_aggregate(Vec::from(["mozzarella"]), 4f64);
}

#[test]
fn test_wildcard_scenario_only() {
    let store = build_and_load();

    let mut query = Query::new();
    let query = query
        .add_wildcard_coordinate(SCENARIO_FIELD_NAME)
        .add_aggregated_measure("price", "sum");

    let qe = QueryEngine::new(&store);
    let result = qe.execute(query);
    result.assert_aggregate(Vec::from([MAIN_SCENARIO_NAME]), 14f64);
    result.assert_aggregate(Vec::from(["s1"]), 13f64);
    result.assert_aggregate(Vec::from(["s2"]), 17f64);
}

#[test]
fn test_wildcard_two_coordinates() {
    let store = build_and_load();

    let mut query = Query::new();
    let query = query
        .add_wildcard_coordinate(SCENARIO_FIELD_NAME)
        .add_wildcard_coordinate("product")
        .add_aggregated_measure("price", "sum");

    let qe = QueryEngine::new(&store);
    let result = qe.execute(query);
    result.assert_aggregate(Vec::from([MAIN_SCENARIO_NAME, "syrup"]), 2f64);
    result.assert_aggregate(Vec::from([MAIN_SCENARIO_NAME, "tofu"]), 8f64);
    result.assert_aggregate(Vec::from([MAIN_SCENARIO_NAME, "mozzarella"]), 4f64);
    result.assert_aggregate(Vec::from(["s1", "syrup"]), 3f64);
    result.assert_aggregate(Vec::from(["s1", "tofu"]), 6f64);
    result.assert_aggregate(Vec::from(["s1", "mozzarella"]), 4f64);
    result.assert_aggregate(Vec::from(["s2", "syrup"]), 4f64);
    result.assert_aggregate(Vec::from(["s2", "tofu"]), 8f64);
    result.assert_aggregate(Vec::from(["s2", "mozzarella"]), 5f64);
}

#[test]
fn test_list_two_coordinates() {
    let store = build_and_load();

    let mut query = Query::new();
    let query = query
        .add_coordinates(SCENARIO_FIELD_NAME, Vec::from(["s2", "s1"]))
        .add_coordinates("product", Vec::from(["syrup"]))
        .add_aggregated_measure("price", "sum");

    let qe = QueryEngine::new(&store);
    let result = qe.execute(query);
    result.assert_aggregate(Vec::from(["s1", "syrup"]), 3f64);
    result.assert_aggregate(Vec::from(["s2", "syrup"]), 4f64);
}

#[test]
fn test_list_three_coordinates() {
    let store = build_and_load();

    let mut query = Query::new();
    let query = query
        .add_coordinates(SCENARIO_FIELD_NAME, Vec::from(["s1", "s2"]))
        .add_coordinates("product", Vec::from(["tofu", "syrup", "mozzarella"]))
        .add_coordinates("category", Vec::from(["milk"]))
        .add_aggregated_measure("price", "sum");

    let qe = QueryEngine::new(&store);
    let result = qe.execute(query);
    result.assert_aggregate(Vec::from(["s1", "tofu", "milk"]), 6f64);
    result.assert_aggregate(Vec::from(["s1", "mozzarella", "milk"]), 4f64);
    result.assert_aggregate(Vec::from(["s2", "tofu", "milk"]), 8f64);
    result.assert_aggregate(Vec::from(["s2", "mozzarella", "milk"]), 5f64);
}

#[test]
fn test_wildcard_on_other_coordinate_and_list_coordinates_on_scenario() {
    let store = build_and_load();

    let mut query = Query::new();
    let query = query
        .add_coordinates(SCENARIO_FIELD_NAME, Vec::from([MAIN_SCENARIO_NAME, "s2"]))
        .add_wildcard_coordinate("product")
        .add_aggregated_measure("price", "sum");

    let qe = QueryEngine::new(&store);
    let result = qe.execute(query);
    assert_eq!(6, result.size());
    result.assert_aggregate(Vec::from([MAIN_SCENARIO_NAME, "syrup"]), 2f64);
    result.assert_aggregate(Vec::from([MAIN_SCENARIO_NAME, "tofu"]), 8f64);
    result.assert_aggregate(Vec::from([MAIN_SCENARIO_NAME, "mozzarella"]), 4f64);
    result.assert_aggregate(Vec::from(["s2", "syrup"]), 4f64);
    result.assert_aggregate(Vec::from(["s2", "tofu"]), 8f64);
    result.assert_aggregate(Vec::from(["s2", "mozzarella"]), 5f64);
}

#[test]
fn test_wildcard_scenario_and_list_coordinates_on_other_coordinate() {
    let store = build_and_load();

    let mut query = Query::new();
    let query = query
        .add_wildcard_coordinate(SCENARIO_FIELD_NAME)
        .add_coordinates("product", Vec::from(["syrup", "tofu"]))
        .add_aggregated_measure("price", "sum");

    let qe = QueryEngine::new(&store);
    let result = qe.execute(query);
    assert_eq!(6, result.size());
    result.assert_aggregate(Vec::from([MAIN_SCENARIO_NAME, "syrup"]), 2f64);
    result.assert_aggregate(Vec::from([MAIN_SCENARIO_NAME, "tofu"]), 8f64);
    result.assert_aggregate(Vec::from(["s1", "syrup"]), 3f64);
    result.assert_aggregate(Vec::from(["s1", "tofu"]), 6f64);
    result.assert_aggregate(Vec::from(["s2", "syrup"]), 4f64);
    result.assert_aggregate(Vec::from(["s2", "tofu"]), 8f64);
}

fn build_and_load() -> Store {
    let schema = Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("product", DataType::Utf8, false),
        Field::new("category", DataType::Utf8, false),
        Field::new("price", DataType::Float64, false),
        Field::new("quantity", DataType::UInt32, false),
    ]);

    let schema_ref = Arc::new(schema);
    let mut store = Store::new(schema_ref, vec![0], CHUNK_DEFAULT_SIZE as u32);

    let main_batch = create_main_batch(&store);
    let s1_batch = create_s1_batch(&store);
    let s2_batch = create_s2_batch(&store);

    store.load(MAIN_SCENARIO_NAME, &main_batch);
    store.load("s1", &s1_batch);
    store.load("s2", &s2_batch);

    // println!("Datastore: {:?}", store);
    // print_batches(&[main_batch]).unwrap();
    // print_batches(&[s1_batch]).unwrap();
    // print_batches(&[s2_batch]).unwrap();
    store
}

fn create_main_batch(store: &Store) -> RecordBatch {
    let id_array = Int64Array::from(vec![0, 1, 2]);
    let quantity_array = UInt32Array::from(vec![5, 3, 4]);
    let product_array = StringArray::from(vec!["syrup", "tofu", "mozzarella"]);
    let category_array = StringArray::from(vec!["condiment", "milk", "milk"]);
    let price_array = Float64Array::from(vec![2f64, 8f64, 4f64]);
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
    let id_array = Int64Array::from(vec![0, 1]);
    let quantity_array = UInt32Array::from(vec![5, 3]);
    let product_array = StringArray::from(vec!["syrup", "tofu"]);
    let category_array = StringArray::from(vec!["condiment", "milk"]);
    let price_array = Float64Array::from(vec![3f64, 6f64]);
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

fn create_s2_batch(store: &Store) -> RecordBatch {
    let id_array = Int64Array::from(vec![0, 2]);
    let quantity_array = UInt32Array::from(vec![5, 4]);
    let product_array = StringArray::from(vec!["syrup", "mozzarella"]);
    let category_array = StringArray::from(vec!["condiment", "milk"]);
    let price_array = Float64Array::from(vec![4f64, 5f64]);
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