//! Cold tier: Parquet file export for old epoch data.
//!
//! Exports tick, transaction, entity, and spectrum data to columnar
//! Parquet files for efficient long-term storage and analytics.

use std::fs;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use anyhow::Result;
use arrow::array::*;
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use parquet::arrow::ArrowWriter;
use parquet::file::properties::WriterProperties;
use tracing::info;

use crate::warm::WarmStorage;

/// Cold tier storage manager.
pub struct ColdStorage {
    base_path: PathBuf,
}

impl ColdStorage {
    pub fn new(base_path: PathBuf) -> Self {
        Self { base_path }
    }

    /// Export all data for a completed epoch to Parquet files.
    pub fn export_epoch(&self, warm: &WarmStorage, epoch: u16, tick_range: (u32, u32)) -> Result<()> {
        let epoch_dir = self.base_path.join(format!("epoch_{epoch}"));
        fs::create_dir_all(&epoch_dir)?;

        info!("Exporting epoch {epoch} (ticks {}..={}) to {:?}", tick_range.0, tick_range.1, epoch_dir);

        self.export_ticks(warm, &epoch_dir, tick_range)?;
        self.export_transactions(warm, &epoch_dir, tick_range)?;

        info!("Epoch {epoch} export complete");
        Ok(())
    }

    /// Export ticks for a range to Parquet.
    fn export_ticks(&self, warm: &WarmStorage, dir: &Path, range: (u32, u32)) -> Result<()> {
        let schema = Arc::new(Schema::new(vec![
            Field::new("tick", DataType::UInt32, false),
            Field::new("epoch", DataType::UInt16, false),
            Field::new("timestamp", DataType::UInt64, false),
            Field::new("transaction_count", DataType::UInt16, false),
            Field::new("data_json", DataType::Utf8, false),
        ]));

        let mut ticks = Vec::new();
        let mut epochs = Vec::new();
        let mut timestamps = Vec::new();
        let mut tx_counts = Vec::new();
        let mut data_jsons = Vec::new();

        // get_tick_range is [from, to] inclusive
        let tick_data = warm.get_tick_range(range.0, range.1)?;
        for (tick_num, data) in &tick_data {
            let val: serde_json::Value = serde_json::from_slice(data)?;
            ticks.push(*tick_num);
            epochs.push(val["epoch"].as_u64().unwrap_or(0) as u16);
            timestamps.push(val["timestamp"].as_u64().unwrap_or(0));
            tx_counts.push(val["transaction_count"].as_u64().unwrap_or(0) as u16);
            data_jsons.push(String::from_utf8_lossy(data).to_string());
        }

        if ticks.is_empty() {
            return Ok(());
        }

        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(UInt32Array::from(ticks)),
                Arc::new(UInt16Array::from(epochs)),
                Arc::new(UInt64Array::from(timestamps)),
                Arc::new(UInt16Array::from(tx_counts)),
                Arc::new(StringArray::from(data_jsons)),
            ],
        )?;

        let file_path = dir.join("ticks.parquet");
        let file = fs::File::create(&file_path)?;
        let props = WriterProperties::builder()
            .set_compression(parquet::basic::Compression::ZSTD(Default::default()))
            .build();
        let mut writer = ArrowWriter::try_new(file, schema, Some(props))?;
        writer.write(&batch)?;
        writer.close()?;

        info!("Exported {} ticks to {:?}", batch.num_rows(), file_path);
        Ok(())
    }

    /// Export transactions for a tick range to Parquet.
    fn export_transactions(&self, warm: &WarmStorage, dir: &Path, range: (u32, u32)) -> Result<()> {
        let schema = Arc::new(Schema::new(vec![
            Field::new("tick", DataType::UInt32, false),
            Field::new("input_type", DataType::UInt16, false),
            Field::new("source", DataType::Utf8, false),
            Field::new("destination", DataType::Utf8, false),
            Field::new("amount", DataType::Int64, false),
            Field::new("data_json", DataType::Utf8, false),
        ]));

        let mut ticks = Vec::new();
        let mut input_types = Vec::new();
        let mut sources = Vec::new();
        let mut destinations = Vec::new();
        let mut amounts = Vec::new();
        let mut data_jsons = Vec::new();

        for tick_num in range.0..=range.1 {
            let hashes = warm.get_tx_hashes_for_tick(tick_num)?;
            for hash in hashes {
                if let Ok(Some(data)) = warm.get_tx(&hash) {
                    let val: serde_json::Value = serde_json::from_slice(&data)?;
                    ticks.push(tick_num);
                    input_types.push(val["input_type"].as_u64().unwrap_or(0) as u16);
                    sources.push(val["source_hex"].as_str().unwrap_or("").to_string());
                    destinations.push(val["destination_hex"].as_str().unwrap_or("").to_string());
                    amounts.push(val["amount"].as_i64().unwrap_or(0));
                    data_jsons.push(String::from_utf8_lossy(&data).to_string());
                }
            }
        }

        if ticks.is_empty() {
            return Ok(());
        }

        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(UInt32Array::from(ticks)),
                Arc::new(UInt16Array::from(input_types)),
                Arc::new(StringArray::from(sources)),
                Arc::new(StringArray::from(destinations)),
                Arc::new(Int64Array::from(amounts)),
                Arc::new(StringArray::from(data_jsons)),
            ],
        )?;

        let file_path = dir.join("transactions.parquet");
        let file = fs::File::create(&file_path)?;
        let props = WriterProperties::builder()
            .set_compression(parquet::basic::Compression::ZSTD(Default::default()))
            .build();
        let mut writer = ArrowWriter::try_new(file, schema, Some(props))?;
        writer.write(&batch)?;
        writer.close()?;

        info!("Exported {} transactions to {:?}", batch.num_rows(), file_path);
        Ok(())
    }

    /// List all exported epochs.
    pub fn list_epochs(&self) -> Result<Vec<u16>> {
        let mut epochs = Vec::new();
        if self.base_path.exists() {
            for entry in fs::read_dir(&self.base_path)? {
                let entry = entry?;
                let name = entry.file_name().to_string_lossy().to_string();
                if let Some(num) = name.strip_prefix("epoch_").and_then(|s| s.parse::<u16>().ok()) {
                    epochs.push(num);
                }
            }
        }
        epochs.sort();
        Ok(epochs)
    }
}
