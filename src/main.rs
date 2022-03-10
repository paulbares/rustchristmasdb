// extern crate core;

// mod chunk_array;
// mod datastore;
// mod dictionary_provider;
// mod row_mapping;
// mod point_dictionary;
// mod aggregator;
// mod point_list_aggregates_result;
// mod query;
// mod query_engine;
// mod bitmap_row_iterable_provider;
// mod row_iterable_provider;

fn main() {
    // let schema = Schema::new(vec![
    //     Field::new("id", DataType::UInt64, false),
    //     Field::new("product", DataType::Utf8, false),
    //     Field::new("category", DataType::Utf8, false),
    //     Field::new("price", DataType::Float64, false),
    //     Field::new("quantity", DataType::UInt32, false),
    // ]);
    //
    // let schema_ref = Arc::new(schema);
    // let mut store = Store::new(schema_ref, vec![0], CHUNK_DEFAULT_SIZE as u32);
    //
    // let main_batch = create_main_batch(&store);
    // let s1_batch = create_s1_batch(&store);
    //
    // store.load(MAIN_SCENARIO_NAME, &main_batch);
    // store.load("s1", &s1_batch);
    //
    // println!("Datastore: {:?}", store);
    // print_batches(&[main_batch]).unwrap();
    //
    // let mut query = Query::new();
    // let query = query
    //     .add_wildcard_coordinate(SCENARIO_FIELD_NAME)
    //     .add_coordinates("category", vec!["condiment", "milk"])
    //     .add_aggregated_measure("price", "sum")
    //     .add_aggregated_measure("quantity", "sum");
    //
    // let qe = QueryEngine::new(&store);
    // let result = qe.execute(query);
    // println!("{}", result);
    // // PointListAggregateResult result = queryEngine.execute(query);
    //
    // // let col = price_aggregator.as_any().downcast_ref::<SumFloat64Aggregator>().unwrap();
    // // println!("{:?}", col.get_destination());
    // // let col = quantity_aggregator.as_any().downcast_ref::<SumUIntAggregator>().unwrap();
    // // println!("{:?}", col.get_destination());
}