
use paimon::catalog::{Identifier};
use paimon::{Catalog, CatalogOptions, FileSystemCatalog, Options};

#[tokio::main]
#[warn(unused)]
async fn main() {
    println!("Hello, world!");
    let catalog = create_file_system_catalog();
    let re = catalog.list_databases().await;
    println!("Databases: {:?}", re);
    let table = get_table_from_catalog(&catalog, "simple_dv_pk_table").await;
    println!("Table Location: {:?}", table.location());
}

fn create_file_system_catalog() -> FileSystemCatalog {
    let warehouse = get_test_warehouse();
    let mut options = Options::new();
    options.set(CatalogOptions::WAREHOUSE, warehouse);
    FileSystemCatalog::new(options).expect("Failed to create FileSystemCatalog")
}
fn get_test_warehouse() -> String {
    std::env::var("PAIMON_TEST_WAREHOUSE").unwrap_or_else(|_| "/share/paimon-warehouse".to_string())
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