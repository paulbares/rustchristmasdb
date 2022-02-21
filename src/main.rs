extern crate core;

use std::sync::Arc;

use arrow::array::{Float64Array, StringArray, UInt64Array};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;

use crate::datastore::{MAIN_SCENARIO_NAME, Store};

mod chunk_array;
mod datastore;
mod dictionary_provider;
mod row_mapping;

fn main() {
    let mut fields = Vec::new();
    fields.push(Field::new("id", DataType::UInt64, false));
    // fields.push(Field::new("product", DataType::Utf8, false));
    // fields.push(Field::new("price", DataType::Float64, false));

    // let tuples = vec![
    //     (0, "syrup", 2),
    //     (1, "tofu", 8),
    //     (2, "mozzarella", 4),
    // ];

    let id_array = UInt64Array::from(vec![0, 1, 2, 3, 4]);
    let product_array =
        StringArray::from(vec!["syrup", "tofu", "mozzarella", "coca cola", "cheese"]);
    let price_array = Float64Array::from(vec![2f64, 8f64, 4f64, 2f64, 10f64]);
    let schema = Schema::new(vec![
        Field::new("id", DataType::UInt64, false),
        Field::new("product", DataType::Utf8, false),
        Field::new("price", DataType::Float64, false),
    ]);

    let schemaRef = Arc::new(schema);
    let datastore = Store::new(schemaRef, vec![0], 4);
    // FIXME create a utility method?
    let batch = RecordBatch::try_new(
        datastore.schema(),
        vec![
            Arc::new(id_array),
            Arc::new(product_array),
            Arc::new(price_array),
        ],
    )
    .unwrap();

    // let col = batch.column(1);
    // let arr = col.as_any().downcast_ref::<StringArray>().unwrap();

    datastore.load(MAIN_SCENARIO_NAME, &batch);

    // println!("batch: {:?}", batch);
    // println!("arr: {:?}", arr);
    println!("Datastore: {:?}", datastore);
    datastore.show();
}
