extern crate core;

use std::sync::Arc;

use arrow::array::{Float64Array, StringArray, UInt32Array, UInt64Array};
use arrow::datatypes::{DataType, Field, Float64Type, Schema, UInt32Type};
use arrow::record_batch::RecordBatch;
use crate::aggregator::{Aggregator, AggregatorAccessor, AggregatorFactory, SumFloat64Aggregator, SumUIntAggregator};

use crate::datastore::{MAIN_SCENARIO_NAME, Store};
use crate::point_dictionary::PointDictionary;

mod chunk_array;
mod datastore;
mod dictionary_provider;
mod row_mapping;
mod point_dictionary;
mod aggregator;

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
    let quantity_array = UInt32Array::from(vec![5, 3, 4, 1, 4]);
    let product_array =
        StringArray::from(vec!["syrup", "tofu", "mozzarella", "syrup", "tofu"]);
    let price_array = Float64Array::from(vec![2f64, 8f64, 4f64, 2f64, 10f64]);
    let schema = Schema::new(vec![
        Field::new("id", DataType::UInt64, false),
        Field::new("product", DataType::Utf8, false),
        Field::new("price", DataType::Float64, false),
        Field::new("quantity", DataType::UInt32, false),
    ]);

    let schema_ref = Arc::new(schema);
    let mut store = Store::new(schema_ref, vec![0], 4);
    // FIXME create a utility method?
    let batch = RecordBatch::try_new(
        store.schema(),
        vec![
            Arc::new(id_array),
            Arc::new(product_array),
            Arc::new(price_array),
            Arc::new(quantity_array),
        ],
    )
    .unwrap();

    store.load(MAIN_SCENARIO_NAME, &batch);

    println!("Datastore: {:?}", store);

    let mut point_dictionary: PointDictionary<[u32; 1]> = PointDictionary::new();

    let factory = AggregatorFactory::new();
    let chunks = store.vector_by_field_by_scenario.get(MAIN_SCENARIO_NAME).unwrap();
    let prod = chunks.get("product").unwrap();
    let price = chunks.get("price").unwrap();
    let quantity = chunks.get("quantity").unwrap();

    let mut price_aggregator: Box<dyn Aggregator> = factory.create(price, "", "");
    let mut quantity_aggregator: Box<dyn Aggregator> = factory.create(quantity, "", "");

    // Try to aggregate by hand.
    for row in 0..*store.row_count.borrow() {
        // let's aggregate by product
        let value: u32 = prod.read::<UInt32Type>(row as u32);
        let mut point: [u32; 1] = [0; 1];
        point[0] = value;
        let destination_row = point_dictionary.map(point);
        price_aggregator.aggregate(row as u32, destination_row);
        quantity_aggregator.aggregate(row as u32, destination_row);
    }
    price_aggregator.finish();
    quantity_aggregator.finish();

    let col = price_aggregator.as_any().downcast_ref::<SumFloat64Aggregator>().unwrap();
    println!("{:?}", col.get_destination());
    let col = quantity_aggregator.as_any().downcast_ref::<SumUIntAggregator>().unwrap();
    println!("{:?}", col.get_destination());
}
