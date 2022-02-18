use arrow::datatypes::{DataType, Field};
use crate::datastore::Store;

mod datastore;
mod chunk_array;
mod row_mapping;

fn main() {
    let mut fields = Vec::new();
    fields.push(Field::new("id", DataType::UInt64, false));
    fields.push(Field::new("product", DataType::Utf8, false));
    fields.push(Field::new("price", DataType::Float64, false));

    let datastore = Store::new(fields, vec![0], 4);
    let tuples = vec![
        (0, "syrup", 2),
        (1, "tofu", 8),
        (2, "mozzarella", 4),
    ];

    datastore.load("main", tuples);

    println!("Datastore: {:?}", datastore);
}
