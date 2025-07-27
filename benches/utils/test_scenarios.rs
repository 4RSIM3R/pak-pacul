use super::data_generator::{DataGenerator, RowType};
use bambang::{
    executor::sequential_scan::SequentialScanner,
    storage::storage_manager::StorageManager,
    types::{error::DatabaseError, row::Row},
    utils::mock::TempDatabase,
};

#[derive(Debug, Clone)]
pub enum TestScenario {
    SingleRowScan { dataset_size: usize, row_type: RowType },
    BatchScan { dataset_size: usize, row_type: RowType, batch_size: usize },
    FullTableScan { dataset_size: usize, row_type: RowType },
    ResetAndRescan { dataset_size: usize, row_type: RowType, partial_scan_count: usize },
    MixedDataTypes { dataset_size: usize },
    MemoryStress { dataset_size: usize },
}

#[derive(Debug, Clone)]
pub struct BenchmarkConfig {
    pub scenario: TestScenario,
    pub iterations: usize,
    pub warmup_iterations: usize,
    pub table_name: String,
}

impl BenchmarkConfig {
    pub fn new(scenario: TestScenario) -> Self {
        Self {
            scenario,
            iterations: 10,
            warmup_iterations: 3,
            table_name: "benchmark_table".to_string(),
        }
    }

    pub fn with_iterations(mut self, iterations: usize) -> Self {
        self.iterations = iterations;
        self
    }

    pub fn with_warmup(mut self, warmup_iterations: usize) -> Self {
        self.warmup_iterations = warmup_iterations;
        self
    }

    pub fn with_table_name(mut self, table_name: String) -> Self {
        self.table_name = table_name;
        self
    }
}

pub struct TestEnvironment {
    pub temp_db: TempDatabase,
    pub config: BenchmarkConfig,
}

impl TestEnvironment {
    pub fn new(config: BenchmarkConfig) -> Result<Self, DatabaseError> {
        let temp_db = TempDatabase::with_prefix("benchmark");
        Ok(Self { temp_db, config })
    }

    pub fn setup_data(&mut self) -> Result<(), DatabaseError> {
        let mut storage = self.temp_db.create_storage_manager().map_err(|e| DatabaseError::Io(std::io::Error::new(std::io::ErrorKind::Other, e.to_string())))?;
        let data_generator = DataGenerator::new();
        match &self.config.scenario {
            TestScenario::SingleRowScan { dataset_size, row_type }
            | TestScenario::BatchScan { dataset_size, row_type, .. }
            | TestScenario::FullTableScan { dataset_size, row_type }
            | TestScenario::ResetAndRescan { dataset_size, row_type, .. } => {
                Self::setup_uniform_data(&mut storage, &self.config.table_name, *dataset_size, *row_type, &data_generator)?
            }
            TestScenario::MixedDataTypes { dataset_size } => {
                Self::setup_mixed_data(&mut storage, &self.config.table_name, *dataset_size, &data_generator)?
            }
            TestScenario::MemoryStress { dataset_size } => {
                Self::setup_uniform_data(&mut storage, &self.config.table_name, *dataset_size, RowType::Large, &data_generator)?
            }
        }
        Ok(())
    }

    fn setup_uniform_data(storage: &mut StorageManager, table_name: &str, dataset_size: usize, row_type: RowType, data_generator: &DataGenerator) -> Result<(), DatabaseError> {
        let create_sql = match row_type {
            RowType::Small => "CREATE TABLE benchmark_table(id INTEGER, name TEXT)",
            RowType::Medium => "CREATE TABLE benchmark_table(id INTEGER, name TEXT, score REAL, active BOOLEAN)",
            RowType::Large => "CREATE TABLE benchmark_table(id INTEGER, description TEXT, data BLOB, metadata TEXT)",
        };
        storage.create_table(table_name, create_sql)?;
        for i in 1..=dataset_size {
            let row = data_generator.generate_row(i as i64, row_type);
            storage.insert_into_table(table_name, row)?;
        }
        Ok(())
    }

    fn setup_mixed_data(storage: &mut StorageManager, table_name: &str, dataset_size: usize, data_generator: &DataGenerator) -> Result<(), DatabaseError> {
        let create_sql = "CREATE TABLE benchmark_table(id INTEGER, name TEXT, score REAL, active BOOLEAN)";
        storage.create_table(table_name, create_sql)?;
        let rows = data_generator.generate_mixed_dataset(dataset_size);
        for row in rows {
            storage.insert_into_table(table_name, row)?;
        }
        Ok(())
    }

    pub fn create_scanner(&mut self, batch_size: Option<usize>) -> Result<SequentialScanner, DatabaseError> {
        let storage = self.temp_db.create_storage_manager().map_err(|e| DatabaseError::Io(std::io::Error::new(std::io::ErrorKind::Other, e.to_string())))?;
        SequentialScanner::new(&storage, self.config.table_name.clone(), batch_size)
    }

    pub fn execute_scenario<F, R>(&self, mut executor: F) -> Result<R, DatabaseError>
    where F: FnMut(&TestEnvironment) -> Result<R, DatabaseError> {
        executor(self)
    }
}

pub fn create_test_scenarios() -> Vec<TestScenario> {
    let dataset_sizes = vec![1_000, 5_000, 10_000, 25_000, 50_000, 100_000];
    let row_types = vec![RowType::Small, RowType::Medium, RowType::Large];
    let batch_sizes = vec![1, 10, 32, 100, 500];
    let mut scenarios = Vec::new();
    for &size in &dataset_sizes {
        for &row_type in &row_types {
            scenarios.push(TestScenario::SingleRowScan { dataset_size: size, row_type });
        }
    }
    for &size in &[10_000, 50_000] {
        for &batch_size in &batch_sizes {
            scenarios.push(TestScenario::BatchScan { dataset_size: size, row_type: RowType::Medium, batch_size });
        }
    }
    for &size in &dataset_sizes {
        scenarios.push(TestScenario::FullTableScan { dataset_size: size, row_type: RowType::Medium });
    }
    for &size in &[10_000, 50_000] {
        scenarios.push(TestScenario::ResetAndRescan { dataset_size: size, row_type: RowType::Medium, partial_scan_count: 100 });
    }
    for &size in &[10_000, 50_000, 100_000] {
        scenarios.push(TestScenario::MixedDataTypes { dataset_size: size });
    }
    for &size in &[1_000, 5_000, 10_000] {
        scenarios.push(TestScenario::MemoryStress { dataset_size: size });
    }
    scenarios
}

pub fn create_focused_scenarios() -> Vec<TestScenario> {
    vec![
        TestScenario::FullTableScan { dataset_size: 100_000, row_type: RowType::Small },
        TestScenario::FullTableScan { dataset_size: 100_000, row_type: RowType::Medium },
        TestScenario::FullTableScan { dataset_size: 100_000, row_type: RowType::Large },
        TestScenario::BatchScan { dataset_size: 50_000, row_type: RowType::Medium, batch_size: 1 },
        TestScenario::BatchScan { dataset_size: 50_000, row_type: RowType::Medium, batch_size: 32 },
        TestScenario::BatchScan { dataset_size: 50_000, row_type: RowType::Medium, batch_size: 500 },
        TestScenario::MemoryStress { dataset_size: 10_000 },
        TestScenario::ResetAndRescan { dataset_size: 25_000, row_type: RowType::Medium, partial_scan_count: 1000 },
    ]
}

impl TestScenario {
    pub fn description(&self) -> String {
        match self {
            TestScenario::SingleRowScan { dataset_size, row_type } => format!("Single row scan: {} {:?} rows", dataset_size, row_type),
            TestScenario::BatchScan { dataset_size, row_type, batch_size } => format!("Batch scan: {} {:?} rows, batch size {}", dataset_size, row_type, batch_size),
            TestScenario::FullTableScan { dataset_size, row_type } => format!("Full table scan: {} {:?} rows", dataset_size, row_type),
            TestScenario::ResetAndRescan { dataset_size, row_type, partial_scan_count } => format!("Reset and rescan: {} {:?} rows, partial scan {}", dataset_size, row_type, partial_scan_count),
            TestScenario::MixedDataTypes { dataset_size } => format!("Mixed data types: {} rows", dataset_size),
            TestScenario::MemoryStress { dataset_size } => format!("Memory stress test: {} large rows", dataset_size),
        }
    }

    pub fn dataset_size(&self) -> usize {
        match self {
            TestScenario::SingleRowScan { dataset_size, .. }
            | TestScenario::BatchScan { dataset_size, .. }
            | TestScenario::FullTableScan { dataset_size, .. }
            | TestScenario::ResetAndRescan { dataset_size, .. }
            | TestScenario::MixedDataTypes { dataset_size }
            | TestScenario::MemoryStress { dataset_size } => *dataset_size,
        }
    }

    pub fn primary_row_type(&self) -> RowType {
        match self {
            TestScenario::SingleRowScan { row_type, .. }
            | TestScenario::BatchScan { row_type, .. }
            | TestScenario::FullTableScan { row_type, .. }
            | TestScenario::ResetAndRescan { row_type, .. } => *row_type,
            TestScenario::MixedDataTypes { .. } => RowType::Medium,
            TestScenario::MemoryStress { .. } => RowType::Large,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_scenario_creation() {
        let scenarios = create_test_scenarios();
        assert!(!scenarios.is_empty());
        let has_single_row = scenarios.iter().any(|s| matches!(s, TestScenario::SingleRowScan { .. }));
        let has_batch = scenarios.iter().any(|s| matches!(s, TestScenario::BatchScan { .. }));
        let has_full_table = scenarios.iter().any(|s| matches!(s, TestScenario::FullTableScan { .. }));
        assert!(has_single_row);
        assert!(has_batch);
        assert!(has_full_table);
    }

    #[test]
    fn test_scenario_descriptions() {
        let scenario = TestScenario::SingleRowScan { dataset_size: 1000, row_type: RowType::Medium };
        let desc = scenario.description();
        assert!(desc.contains("1000"));
        assert!(desc.contains("Medium"));
    }

    #[test]
    fn test_benchmark_config() {
        let scenario = TestScenario::FullTableScan { dataset_size: 5000, row_type: RowType::Small };
        let config = BenchmarkConfig::new(scenario).with_iterations(20).with_warmup(5);
        assert_eq!(config.iterations, 20);
        assert_eq!(config.warmup_iterations, 5);
    }
}
