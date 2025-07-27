use bambang::{
    storage::storage_manager::StorageManager,
    types::error::DatabaseError,
    utils::mock::TempDatabase,
};
use criterion::{BenchmarkId, Criterion, Throughput, criterion_group, criterion_main};
mod utils;
use utils::benchmark_helpers::
    measure_scan_operation
;
use utils::data_generator::{DataGenerator, RowType};

const DATASET_SIZES: &[usize] = &[50, 100];
const ROW_TYPES: &[RowType] = &[RowType::Small, RowType::Medium, RowType::Large];

fn setup_test_table(
    storage: &mut StorageManager,
    table_name: &str,
    row_count: usize,
    row_type: RowType,
) -> Result<(), DatabaseError> {
    let create_sql = match row_type {
        RowType::Small => "CREATE TABLE test_table(id INTEGER, name TEXT)",
        RowType::Medium => {
            "CREATE TABLE test_table(id INTEGER, name TEXT, score REAL, active BOOLEAN)"
        }
        RowType::Large => {
            "CREATE TABLE test_table(id INTEGER, description TEXT, data BLOB, metadata TEXT)"
        }
    };
    storage.create_table(table_name, create_sql)?;
    let data_generator = DataGenerator::new();
    for i in 1..=row_count {
        let row = data_generator.generate_row(i as i64, row_type);
        storage.insert_into_table(table_name, row)?;
    }
    Ok(())
}

fn benchmark_sequential_scan_throughput(c: &mut Criterion) {
    let mut group = c.benchmark_group("sequential_scan_throughput");

    for &dataset_size in DATASET_SIZES {
        for &row_type in ROW_TYPES {
            let mut temp_db = TempDatabase::with_prefix("bench_throughput");
            let mut storage = temp_db.create_storage_manager().unwrap();
            setup_test_table(&mut storage, "test_table", dataset_size, row_type).unwrap();

            let benchmark_id =
                BenchmarkId::from_parameter(format!("{}_{:?}", dataset_size, row_type));
            group.throughput(Throughput::Elements(dataset_size as u64));

            group.bench_with_input(benchmark_id, &(dataset_size, row_type), |b, &(size, _)| {
                b.iter(|| measure_scan_operation(&storage, "test_table", size).unwrap());
            });
        }
    }
    group.finish();
}

criterion_group!(
    benches,
    benchmark_sequential_scan_throughput,
);

criterion_main!(benches);
