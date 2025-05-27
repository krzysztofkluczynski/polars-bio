use arrow::array::{Array, Int64Array, LargeStringArray};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use datafusion::dataframe::DataFrame;
use datafusion::error::{DataFusionError, Result};
use exon::ExonSession;
use std::collections::HashMap;
use std::error::Error;
use std::sync::{Arc, Mutex};
use std::thread;
use tokio::runtime::Runtime;

/// Reverse complement of a DNA sequence
fn reverse_complement(seq: &[u8]) -> Vec<u8> {
    seq.iter()
        .rev()
        .map(|b| match b {
            b'A' | b'a' => b'T',
            b'T' | b't' => b'A',
            b'C' | b'c' => b'G',
            b'G' | b'g' => b'C',
            _ => b'N',
        })
        .collect()
}

/// Canonical form of a k-mer
fn canonical_kmer(kmer: &[u8]) -> Vec<u8> {
    let rc = reverse_complement(kmer);
    if rc.as_slice() < kmer {
        rc
    } else {
        kmer.to_vec()
    }
}

/// Count canonical k-mers from a table and return a DataFrame
pub fn compute_kmers(
    ctx: &ExonSession,
    rt: &Runtime,
    table_name: String,
    k: usize,
) -> Result<DataFrame, Box<dyn Error>> {
    let query = format!("SELECT sequence FROM {}", table_name);
    let df = rt.block_on(ctx.sql(&query))?;
    let batches = rt.block_on(df.collect())?;

    let global_counts: Arc<Mutex<HashMap<String, i64>>> = Arc::new(Mutex::new(HashMap::new()));
    let mut handles = vec![];

    for batch in batches {
        let counts_ref = Arc::clone(&global_counts);
        let handle = thread::spawn(move || {
            let col_idx = match batch.schema().index_of("sequence") {
                Ok(idx) => idx,
                Err(_) => return,
            };

            let array = match batch.column(col_idx).as_any().downcast_ref::<LargeStringArray>() {
                Some(arr) => arr,
                None => return,
            };

            let mut local_counts = HashMap::new();

            for i in 0..array.len() {
                if !array.is_valid(i) {
                    continue;
                }

                let seq = array.value(i).as_bytes();
                if seq.len() < k {
                    continue;
                }

                for j in 0..=seq.len() - k {
                    let kmer = &seq[j..j + k];
                    if kmer.iter().any(|&b| {
                        !matches!(b, b'A' | b'C' | b'G' | b'T' | b'a' | b'c' | b'g' | b't')
                    }) {
                        continue;
                    }

                    let canonical = canonical_kmer(kmer);
                    let kmer_str = String::from_utf8_lossy(&canonical).to_string();
                    *local_counts.entry(kmer_str).or_insert(0) += 1;
                }
            }

            let mut shared = counts_ref.lock().unwrap();
            for (k, v) in local_counts {
                *shared.entry(k).or_insert(0) += v;
            }
        });

        handles.push(handle);
    }

    for handle in handles {
        handle.join().unwrap();
    }

    // Prepare result RecordBatch
    let counts = Arc::try_unwrap(global_counts).unwrap().into_inner().unwrap();
    let kmers: Vec<&str> = counts.keys().map(|s| s.as_str()).collect();
    let count_vals: Vec<i64> = kmers.iter().map(|k| *counts.get(*k).unwrap_or(&0)).collect();

    let schema = Arc::new(Schema::new(vec![
        Field::new("kmer", DataType::LargeUtf8, false),
        Field::new("count", DataType::Int64, false),
    ]));

    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(LargeStringArray::from(kmers)) as _,
            Arc::new(Int64Array::from(count_vals)) as _,
        ],
    )?;

    ctx.session.register_batch("kmers_result", batch).ok();
    let df = rt.block_on(ctx.session.table("kmers_result"))?;
    Ok(df)
}
