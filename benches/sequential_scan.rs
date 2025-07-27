use criterion::{criterion_group, criterion_main, BenchmarkId, Criterion, Throughput};
use memory_stats::memory_stats;
use std::{hint::black_box, time::Instant};
use bambang::{
    executor::{scan::Scanner, sequential_scan::SequentialScanner},
    storage::storage_manager::StorageManager,
    types::error::DatabaseError,
    utils::mock::TempDatabase,
};
mod utils;
use utils::data_generator::{DataGenerator, RowType};

const DATASET_SIZES: &[usize] = &[1_000, 5_000, 10_000, 25_000, 50_000, 100_000];
const BATCH_SIZES: &[usize] = &[1, 10, 32, 100, 500];
const ROW_TYPES: &[RowType] = &[RowType::Small, RowType::Medium, RowType::Large];

fn setup_test_table(storage: &mut StorageManager, table_name: &str, row_count: usize, row_type: RowType) -> Result<(), DatabaseError> {
    let create_sql = match row_type {
        RowType::Small => "CREATE TABLE test_table(id INTEGER, name TEXT)",
        RowType::Medium => "CREATE TABLE test_table(id INTEGER, name TEXT, score REAL, active BOOLEAN)",
        RowType::Large => "CREATE TABLE test_table(id INTEGER, description TEXT, data BLOB, metadata TEXT)",
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
            let benchmark_id = BenchmarkId::from_parameter(format!("{}_{:?}", dataset_size, row_type));
            group.throughput(Throughput::Elements(dataset_size as u64));
            group.bench_with_input(benchmark_id, &(dataset_size, row_type), |b, &(size, row_type)| {
                b.iter_custom(|iters| {
                    let mut total_duration = std::time::Duration::new(0, 0);
                    for _ in 0..iters {
                        let mut temp_db = TempDatabase::with_prefix("bench_throughput");
                        let mut storage = temp_db.create_storage_manager().unwrap();
                        setup_test_table(&mut storage, "test_table", size, row_type).unwrap();
                        let mut scanner = SequentialScanner::new(&storage, "test_table".to_string(), None).unwrap();
                        let start = Instant::now();
                        let mut count = 0;
                        while let Some(_row) = black_box(scanner.scan().unwrap()) {
                            count += 1;
                        }
                        let duration = start.elapsed();
                        assert_eq!(count, size);
                        total_duration += duration;
                    }
                    total_duration
                });
            });
        }
    }
    group.finish();
}

fn benchmark_batch_scan_performance(c: &mut Criterion) {
    let mut group = c.benchmark_group("batch_scan_performance");
    for &dataset_size in &[10_000, 50_000] {
        for &batch_size in BATCH_SIZES {
            let benchmark_id = BenchmarkId::from_parameter(format!("{}rows_{}batch", dataset_size, batch_size));
            group.throughput(Throughput::Elements(dataset_size as u64));
            group.bench_with_input(benchmark_id, &(dataset_size, batch_size), |b, &(size, batch_size)| {
                b.iter_custom(|iters| {
                    let mut total_duration = std::time::Duration::new(0, 0);
                    for _ in 0..iters {
                        let mut temp_db = TempDatabase::with_prefix("bench_batch");
                        let mut storage = temp_db.create_storage_manager().unwrap();
                        setup_test_table(&mut storage, "test_table", size, RowType::Medium).unwrap();
                        let mut scanner = SequentialScanner::new(&storage, "test_table".to_string(), Some(batch_size)).unwrap();
                        let start = Instant::now();
                        let mut total_rows = 0;
                        loop {
                            let batch = black_box(scanner.scan_batch(batch_size).unwrap());
                            if batch.is_empty() {
                                break;
                            }
                            total_rows += batch.len();
                        }
                        let duration = start.elapsed();
                        assert_eq!(total_rows, size);
                        total_duration += duration;
                    }
                    total_duration
                });
            });
        }
    }
    group.finish();
}

fn benchmark_memory_usage(c: &mut Criterion) {
    let mut group = c.benchmark_group("memory_usage");
    for &dataset_size in &[10_000, 50_000, 100_000] {
        let benchmark_id = BenchmarkId::from_parameter(dataset_size);
        group.bench_with_input(benchmark_id, &dataset_size, |b, &size| {
            b.iter_custom(|iters| {
                let mut total_duration = std::time::Duration::new(0, 0);
                for _ in 0..iters {
                    let mut temp_db = TempDatabase::with_prefix("bench_memory");
                    let mut storage = temp_db.create_storage_manager().unwrap();
                    setup_test_table(&mut storage, "test_table", size, RowType::Medium).unwrap();
                    let memory_before = memory_stats().map(|m| m.physical_mem).unwrap_or(0);
                    let mut scanner = SequentialScanner::new(&storage, "test_table".to_string(), None).unwrap();
                    let start = Instant::now();
                    let mut count = 0;
                    let mut peak_memory = memory_before;
                    while let Some(_row) = black_box(scanner.scan().unwrap()) {
                        count += 1;
                        if count % 1000 == 0 {
                            if let Some(current_memory) = memory_stats().map(|m| m.physical_mem) {
                                peak_memory = peak_memory.max(current_memory);
                            }
                        }
                    }
                    let duration = start.elapsed();
                    let memory_after = memory_stats().map(|m| m.physical_mem).unwrap_or(0);
                    let memory_delta = memory_after.saturating_sub(memory_before);
                    let peak_delta = peak_memory.saturating_sub(memory_before);
                    eprintln!("Dataset: {} rows, Memory delta: {} bytes, Peak delta: {} bytes", size, memory_delta, peak_delta);
                    assert_eq!(count, size);
                    total_duration += duration;
                }
                total_duration
            });
        });
    }
    group.finish();
}

fn benchmark_scan_latency(c: &mut Criterion) {
    let mut group = c.benchmark_group("scan_latency");
    for &dataset_size in &[1_000, 10_000, 50_000] {
        let benchmark_id = BenchmarkId::from_parameter(dataset_size);
        group.bench_with_input(benchmark_id, &dataset_size, |b, &size| {
            let mut temp_db = TempDatabase::with_prefix("bench_latency");
            let mut storage = temp_db.create_storage_manager().unwrap();
            setup_test_table(&mut storage, "test_table", size, RowType::Medium).unwrap();
            b.iter(|| {
                let mut scanner = SequentialScanner::new(&storage, "test_table".to_string(), None).unwrap();
                let start = Instant::now();
                let _first_row = black_box(scanner.scan().unwrap());
                let first_scan_duration = start.elapsed();
                let mut count = 1;
                while let Some(_row) = black_box(scanner.scan().unwrap()) {
                    count += 1;
                }
                assert_eq!(count, size);
                first_scan_duration
            });
        });
    }
    group.finish();
}

fn benchmark_reset_performance(c: &mut Criterion) {
    let mut group = c.benchmark_group("reset_performance");
    for &dataset_size in &[10_000, 50_000] {
        let benchmark_id = BenchmarkId::from_parameter(dataset_size);
        group.bench_with_input(benchmark_id, &dataset_size, |b, &size| {
            let mut temp_db = TempDatabase::with_prefix("bench_reset");
            let mut storage = temp_db.create_storage_manager().unwrap();
            setup_test_table(&mut storage, "test_table", size, RowType::Medium).unwrap();
            b.iter(|| {
                let mut scanner = SequentialScanner::new(&storage, "test_table".to_string(), None).unwrap();
                for _ in 0..100 {
                    let _ = black_box(scanner.scan().unwrap());
                }
                let start = Instant::now();
                black_box(scanner.reset().unwrap());
                let reset_duration = start.elapsed();
                let mut count = 0;
                while let Some(_row) = scanner.scan().unwrap() {
                    count += 1;
                }
                assert_eq!(count, size);
                reset_duration
            });
        });
    }
    group.finish();
}

criterion_group!(
    benches,
    benchmark_sequential_scan_throughput,
    benchmark_batch_scan_performance,
    benchmark_memory_usage,
    benchmark_scan_latency,
    benchmark_reset_performance
);

criterion_main!(benches);
