use std::sync::Arc;
use arrow::array::{Float64Array, Int64Array, StringArray, UInt32Array, UInt64Array};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use arrow::util::pretty::print_batches;
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
    result.assertAggregate(Vec::from(["syrup"]), 2f64);
    result.assertAggregate(Vec::from(["tofu"]), 8f64);
    result.assertAggregate(Vec::from(["mozzarella"]), 4f64);
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
    println!("{}", result);
    result.assertAggregate(Vec::from(["base"]), 14f64);
    result.assertAggregate(Vec::from(["s1"]), 13f64);
    result.assertAggregate(Vec::from(["s2"]), 17f64);
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
    print_batches(&[main_batch]).unwrap();
    print_batches(&[s1_batch]).unwrap();
    print_batches(&[s2_batch]).unwrap();
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