use arrow_array::RecordBatch;
use paimon::catalog::{Identifier};
use paimon::{Catalog, CatalogOptions, FileSystemCatalog, Options, Plan};
use futures::TryStreamExt;

#[tokio::main]
async fn main() {
    println!("Hello, world!");
    let catalog = create_file_system_catalog();
    let re = catalog.list_databases().await;
    println!("Databases: {:?}", re);
    let table = get_table_from_catalog(&catalog, "simple_dv_pk_table").await;
    println!("Table Location: {:?}", table.location());
}
//
// async fn scan_and_read<C: Catalog + ?Sized>(
//     catalog: &C,
//     table_name: &str,
//     projection: Option<&[&str]>,
// ) -> (Plan, Vec<RecordBatch>) {
//     let table = get_table_from_catalog(catalog, table_name).await;
//
//     let mut read_builder = table.new_read_builder();
//     if let Some(cols) = projection {
//         read_builder.with_projection(cols);
//     }
//     let scan = read_builder.new_scan();
//     let plan = scan.plan().await.expect("Failed to plan scan");
//
//     let read = read_builder.new_read().expect("Failed to create read");
//     let stream = read
//         .to_arrow(plan.splits())
//         .expect("Failed to create arrow stream");
//     let batches: Vec<_> = stream
//         .try_collect()
//         .await
//         .expect("Failed to collect batches");
//
//     assert!(
//         !batches.is_empty(),
//         "Expected at least one batch from table {table_name}"
//     );
//     (plan, batches)
// }

fn create_file_system_catalog() -> FileSystemCatalog {
    let warehouse = get_test_warehouse();
    let mut options = Options::new();
    options.set(CatalogOptions::WAREHOUSE, warehouse);
    FileSystemCatalog::new(options).expect("Failed to create FileSystemCatalog")
}
fn get_test_warehouse() -> String {
    std::env::var("PAIMON_TEST_WAREHOUSE").unwrap_or_else(|_| "/tmp/paimon-warehouse".to_string())
}

async fn get_table_from_catalog<C: Catalog + ?Sized>(
    catalog: &C,
    table_name: &str,
) -> paimon::Table {
    let identifier = Identifier::new("default", table_name);
    catalog
        .get_table(&identifier)
        .await
        .expect("Failed to get table")
}