use polars::prelude::*;
use std::iter::FromIterator;

fn main() -> PolarsResult<()> {
    let schema = Schema::from_iter(vec![
        Field::new("id", DataType::Utf8),
        Field::new("value", DataType::Float64),
    ]);

    let df = CsvReader::from_path("data.csv")?
        .has_header(false)
        .with_schema(schema.into())
        .finish()?;

    println!("{}", df);

    Ok(())
}
