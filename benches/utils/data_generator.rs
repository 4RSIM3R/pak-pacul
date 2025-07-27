use bambang::types::{row::Row, value::Value};

#[derive(Debug, Clone, Copy)]
pub enum RowType {
    Small,
    Medium,
    Large,
}

pub struct DataGenerator {
    seed: u64,
}

impl DataGenerator {
    pub fn new() -> Self {
        Self { seed: 42 }
    }

    pub fn with_seed(seed: u64) -> Self {
        Self { seed }
    }

    pub fn generate_row(&self, id: i64, row_type: RowType) -> Row {
        match row_type {
            RowType::Small => self.generate_small_row(id),
            RowType::Medium => self.generate_medium_row(id),
            RowType::Large => self.generate_large_row(id),
        }
    }

    fn generate_small_row(&self, id: i64) -> Row {
        Row::new(vec![Value::Integer(id), Value::Text("short".to_string())])
    }

    fn generate_medium_row(&self, id: i64) -> Row {
        Row::new(vec![
            Value::Integer(id),
            Value::Text(format!("user_name_{}", id)),
            Value::Real(id as f64 * 1.5 + 0.1),
            Value::Boolean(id % 2 == 0),
        ])
    }

    fn generate_large_row(&self, id: i64) -> Row {
        let large_text = format!(
            "This is a large text field for row {} containing substantial data to test performance with larger row sizes. {}",
            id,
            "x".repeat(400)
        );
        let mut blob_data = Vec::with_capacity(200);
        for i in 0..200 {
            blob_data.push(((id + i as i64) % 256) as u8);
        }
        let metadata = format!(
            "{{\"id\":{},\"timestamp\":{},\"version\":\"1.0\",\"tags\":[\"test\",\"benchmark\",\"row_{}\"]}}",
            id,
            1640995200 + id,
            id
        );
        Row::new(vec![
            Value::Integer(id),
            Value::Text(large_text),
            Value::Blob(blob_data),
            Value::Text(metadata),
        ])
    }

    pub fn generate_rows(&self, count: usize, row_type: RowType) -> Vec<Row> {
        (1..=count)
            .map(|i| self.generate_row(i as i64, row_type))
            .collect()
    }

    pub fn estimate_row_size(&self, row_type: RowType) -> usize {
        match row_type {
            RowType::Small => 8 + 5 + 16,
            RowType::Medium => 8 + 20 + 8 + 1 + 24,
            RowType::Large => 8 + 500 + 200 + 100 + 32,
        }
    }

    pub fn generate_mixed_dataset(&self, total_rows: usize) -> Vec<Row> {
        let mut rows = Vec::with_capacity(total_rows);
        for i in 1..=total_rows {
            let row_type = match i % 10 {
                0..=6 => RowType::Small,
                7..=8 => RowType::Medium,
                _ => RowType::Large,
            };
            rows.push(self.generate_row(i as i64, row_type));
        }
        rows
    }

    pub fn generate_with_distribution(
        &self,
        total_rows: usize,
        small_pct: f32,
        medium_pct: f32,
        large_pct: f32,
    ) -> Vec<Row> {
        assert!((small_pct + medium_pct + large_pct - 1.0).abs() < 0.001);
        let mut rows = Vec::with_capacity(total_rows);
        for i in 1..=total_rows {
            let rand_val = ((i as u64 * self.seed) % 1000) as f32 / 1000.0;
            let row_type = if rand_val < small_pct {
                RowType::Small
            } else if rand_val < small_pct + medium_pct {
                RowType::Medium
            } else {
                RowType::Large
            };
            rows.push(self.generate_row(i as i64, row_type));
        }
        rows
    }
}

impl Default for DataGenerator {
    fn default() -> Self {
        Self::new()
    }
}
