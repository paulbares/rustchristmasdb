use std::collections::HashMap;
use std::fs::File;
use std::sync::Arc;
use std::time::Duration;
use arrow::array::{ArrayRef, Int64Array, Int64Builder};
use arrow::csv;
use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use arrow::record_batch::RecordBatch;
use criterion::{criterion_group, criterion_main, Criterion, black_box};
use rustchristmasdb::datastore::{CHUNK_DEFAULT_SIZE, MAIN_SCENARIO_NAME, SCENARIO_FIELD_NAME, Store};
use rustchristmasdb::query::Query;
use rustchristmasdb::query_engine::QueryEngine;

const N: i32 = 10_000;
const KEY_INDICES: [u32; 1] = [4; 1];

fn criterion_benchmark(c: &mut Criterion) {
    let store = load();
    c
        .bench_function("execute_query", |b| b.iter(|| execute_query(&store)));
}

fn execute_query(store: &Store) {
    let mut query = Query::new();
    let query = query
        .add_wildcard_coordinate(SCENARIO_FIELD_NAME)
        .add_coordinates("CategoryName", Vec::from(["Condiments", "Beverages"]))
        .add_aggregated_measure("Price", "sum")
        .add_aggregated_measure("Quantity", "sum");
    let qe = QueryEngine::new(&store);
    let result = qe.execute(query);
    black_box(result);
    // println!("{}", result);
}

fn load() -> Store {
    let schema = create_schema();
    let schema_ref: SchemaRef = Arc::new(schema);
    let mut m: HashMap<&str, Vec<RecordBatch>> = HashMap::new();
    for i in 0..N {
        if i == 0 {
            for scenario in list_of_scenarios() {
                let path = format!("test/data/data_{}_scenario.csv", scenario);
                let file = File::open(path).unwrap();
                let mut csv = csv::Reader::new(file, Arc::clone(&schema_ref), true, None, 1024, None, None, None);
                let batch = csv.next().unwrap().unwrap();
                let mut vec = Vec::new();
                vec.push(batch);
                m.insert(scenario, vec);
            }
        } else {
            for scenario in list_of_scenarios() {
                let batch = &m.get(scenario).unwrap()[0];
                let mut new_columns: Vec<ArrayRef> = Vec::new();
                for index in 0..batch.num_columns() {
                    let col = batch.column(index);
                    if index == KEY_INDICES[0] as usize {
                        // Create a new column
                        let arr = col.as_any().downcast_ref::<Int64Array>().unwrap();
                        let mut builder = Int64Builder::new(batch.num_rows());
                        for current in arr.values() {
                            let x: i64 = current + (batch.num_rows() as i64) * (i as i64);
                            builder.append_value(x).unwrap();
                        }
                        let new_index = builder.finish();
                        new_columns.push(Arc::new(new_index));
                    } else {
                        new_columns.push(col.clone());
                    }
                }
                let new_batch = RecordBatch::try_new(
                    schema_ref.clone(),
                    new_columns).unwrap();
                m.get_mut(scenario).unwrap().push(new_batch);
            }
        }
    }

    let mut store = create_store();
    for scenario in list_of_scenarios() {
        let batches = m.get(scenario).unwrap();
        let new_batch = RecordBatch::concat(&schema_ref, batches.as_slice()).unwrap();
        store.load(scenario, &new_batch);
    }
    store
}

fn create_schema() -> Schema {
    return Schema::new(vec![
        Field::new("OrderId", DataType::UInt64, false),
        Field::new("CustomerID", DataType::UInt64, false),
        Field::new("EmployeeID", DataType::UInt64, false),
        Field::new("OrderDate", DataType::Utf8, false),
        Field::new("OrderDetailID", DataType::Int64, false),
        Field::new("Quantity", DataType::UInt32, false),
        Field::new("ProductName", DataType::Utf8, false),
        Field::new("Unit", DataType::Utf8, false),
        Field::new("Price", DataType::Float64, false),
        Field::new("CategoryName", DataType::Utf8, false),
        Field::new("SupplierName", DataType::Utf8, false),
        Field::new("City", DataType::Utf8, false),
        Field::new("Country", DataType::Utf8, false),
        Field::new("ShipperName", DataType::Utf8, false),
    ]);
}

fn create_store() -> Store {
    Store::new(Arc::new(create_schema()), Vec::from(KEY_INDICES), CHUNK_DEFAULT_SIZE as u32)
}

fn list_of_scenarios() -> Vec<&'static str> {
    vec![MAIN_SCENARIO_NAME, "s50", "s25", "s10", "s05"]
}

criterion_group! {
    name = benches;
    config = Criterion::default().measurement_time(Duration::from_secs(100));
    targets = criterion_benchmark
}
// criterion_group!(benches, criterion_benchmark);
criterion_main!(benches);