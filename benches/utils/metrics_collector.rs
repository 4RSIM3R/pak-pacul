use std::time::{Duration, Instant};
use memory_stats::memory_stats;
use sysinfo::{System, Process, Pid};

#[derive(Debug, Clone)]
pub struct BenchmarkMetrics {
    pub duration: Duration,
    pub rows_processed: usize,
    pub throughput: f64,
    pub memory: MemoryMetrics,
    pub io: IoMetrics,
    pub timing: TimingMetrics,
}

#[derive(Debug, Clone)]
pub struct MemoryMetrics {
    pub memory_before: usize,
    pub memory_after: usize,
    pub peak_memory: usize,
    pub memory_delta: i64,
    pub peak_delta: i64,
}

#[derive(Debug, Clone)]
pub struct IoMetrics {
    pub disk_reads: u64,
    pub bytes_read: u64,
    pub disk_writes: u64,
    pub bytes_written: u64,
}

#[derive(Debug, Clone)]
pub struct TimingMetrics {
    pub first_scan_time: Option<Duration>,
    pub avg_time_per_row: Duration,
    pub reset_time: Option<Duration>,
    pub init_time: Option<Duration>,
}

pub struct MetricsCollector {
    start_time: Option<Instant>,
    memory_before: Option<usize>,
    peak_memory: usize,
    rows_processed: usize,
    system: System,
    process_id: Pid,
    io_before: Option<IoMetrics>,
    first_scan_time: Option<Duration>,
    init_time: Option<Duration>,
    reset_time: Option<Duration>,
}

impl MetricsCollector {
    pub fn new() -> Self {
        let mut system = System::new_all();
        system.refresh_all();
        let process_id = sysinfo::get_current_pid().unwrap_or(Pid::from_u32(0));
        Self {
            start_time: None,
            memory_before: None,
            peak_memory: 0,
            rows_processed: 0,
            system,
            process_id,
            io_before: None,
            first_scan_time: None,
            init_time: None,
            reset_time: None,
        }
    }

    pub fn start(&mut self) {
        self.start_time = Some(Instant::now());
        self.memory_before = memory_stats().map(|m| m.physical_mem);
        self.peak_memory = self.memory_before.unwrap_or(0);
        self.system.refresh_all();
        if let Some(process) = self.system.process(self.process_id) {
            let disk_usage = process.disk_usage();
            self.io_before = Some(IoMetrics {
                disk_reads: disk_usage.read_bytes,
                bytes_read: disk_usage.total_read_bytes,
                disk_writes: disk_usage.written_bytes,
                bytes_written: disk_usage.total_written_bytes,
            });
        }
    }

    pub fn record_init_time(&mut self, duration: Duration) {
        self.init_time = Some(duration);
    }

    pub fn record_first_scan_time(&mut self, duration: Duration) {
        self.first_scan_time = Some(duration);
    }

    pub fn record_reset_time(&mut self, duration: Duration) {
        self.reset_time = Some(duration);
    }

    pub fn increment_rows(&mut self, count: usize) {
        self.rows_processed += count;
        if self.rows_processed % 1000 == 0 {
            if let Some(current_memory) = memory_stats().map(|m| m.physical_mem) {
                self.peak_memory = self.peak_memory.max(current_memory);
            }
        }
    }

    pub fn finish(&mut self) -> BenchmarkMetrics {
        let duration = self.start_time.map(|start| start.elapsed()).unwrap_or(Duration::from_secs(0));
        let memory_after = memory_stats().map(|m| m.physical_mem).unwrap_or(0);
        let memory_before = self.memory_before.unwrap_or(0);
        self.system.refresh_all();
        let io_after = if let Some(process) = self.system.process(self.process_id) {
            let disk_usage = process.disk_usage();
            IoMetrics {
                disk_reads: disk_usage.read_bytes,
                bytes_read: disk_usage.total_read_bytes,
                disk_writes: disk_usage.written_bytes,
                bytes_written: disk_usage.total_written_bytes,
            }
        } else {
            IoMetrics {
                disk_reads: 0,
                bytes_read: 0,
                disk_writes: 0,
                bytes_written: 0,
            }
        };
        let io_delta = if let Some(io_before) = &self.io_before {
            IoMetrics {
                disk_reads: io_after.disk_reads.saturating_sub(io_before.disk_reads),
                bytes_read: io_after.bytes_read.saturating_sub(io_before.bytes_read),
                disk_writes: io_after.disk_writes.saturating_sub(io_before.disk_writes),
                bytes_written: io_after.bytes_written.saturating_sub(io_before.bytes_written),
            }
        } else {
            io_after
        };
        let throughput = if duration.as_secs_f64() > 0.0 {
            self.rows_processed as f64 / duration.as_secs_f64()
        } else {
            0.0
        };
        let avg_time_per_row = if self.rows_processed > 0 {
            duration / self.rows_processed as u32
        } else {
            Duration::from_secs(0)
        };
        BenchmarkMetrics {
            duration,
            rows_processed: self.rows_processed,
            throughput,
            memory: MemoryMetrics {
                memory_before,
                memory_after,
                peak_memory: self.peak_memory,
                memory_delta: memory_after as i64 - memory_before as i64,
                peak_delta: self.peak_memory as i64 - memory_before as i64,
            },
            io: io_delta,
            timing: TimingMetrics {
                first_scan_time: self.first_scan_time,
                avg_time_per_row,
                reset_time: self.reset_time,
                init_time: self.init_time,
            },
        }
    }

    pub fn current_memory_usage(&self) -> usize {
        memory_stats().map(|m| m.physical_mem).unwrap_or(0)
    }

    pub fn sample_memory(&mut self) {
        if let Some(current_memory) = memory_stats().map(|m| m.physical_mem) {
            self.peak_memory = self.peak_memory.max(current_memory);
        }
    }
}

impl Default for MetricsCollector {
    fn default() -> Self {
        Self::new()
    }
}

impl BenchmarkMetrics {
    pub fn format_summary(&self) -> String {
        format!(
            "Benchmark Results:\n\
             Duration: {:.2}s\n\
             Rows Processed: {}\n\
             Throughput: {:.2} rows/sec\n\
             Memory Delta: {} bytes ({:.2} MB)\n\
             Peak Memory Delta: {} bytes ({:.2} MB)\n\
             Avg Time/Row: {:.2}μs\n\
             Disk Reads: {} bytes\n\
             Disk Writes: {} bytes",
            self.duration.as_secs_f64(),
            self.rows_processed,
            self.throughput,
            self.memory.memory_delta,
            self.memory.memory_delta as f64 / 1024.0 / 1024.0,
            self.memory.peak_delta,
            self.memory.peak_delta as f64 / 1024.0 / 1024.0,
            self.timing.avg_time_per_row.as_micros(),
            self.io.bytes_read,
            self.io.bytes_written
        )
    }

    pub fn to_csv_row(&self) -> String {
        format!(
            "{},{},{},{},{},{},{},{},{},{},{}",
            self.duration.as_secs_f64(),
            self.rows_processed,
            self.throughput,
            self.memory.memory_before,
            self.memory.memory_after,
            self.memory.peak_memory,
            self.memory.memory_delta,
            self.memory.peak_delta,
            self.timing.avg_time_per_row.as_micros(),
            self.io.bytes_read,
            self.io.bytes_written
        )
    }

    pub fn csv_header() -> &'static str {
        "duration_sec,rows_processed,throughput_rows_per_sec,memory_before,memory_after,peak_memory,memory_delta,peak_delta,avg_time_per_row_micros,bytes_read,bytes_written"
    }
}