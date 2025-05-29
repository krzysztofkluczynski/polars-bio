use std::collections::HashMap;
use std::sync::Arc;

use arrow_array::{ArrayRef, StringArray, UInt64Array, StructArray};
use arrow_array::builder::{StringBuilder, UInt64Builder};
use arrow_schema::{DataType, Field, Fields};
use datafusion::common::Result;
use datafusion::logical_expr::{Volatility, AggregateUDF, create_udaf};
use datafusion::physical_plan::Accumulator;
use datafusion::logical_expr::function::AccumulatorArgs;
use datafusion::scalar::ScalarValue;
use arrow_array::Array;

pub fn create_kmer_count_udaf(k: usize) -> AggregateUDF {
    let accumulator_creator = move |_args: AccumulatorArgs<'_>| -> Result<Box<dyn Accumulator>> {
        Ok(Box::new(KmerCountAccumulator::new(k)))
    };

    let return_type = Arc::new(DataType::Struct(Fields::from(vec![
        Field::new("kmer", DataType::Utf8, false),
        Field::new("count", DataType::UInt64, false),
    ])));

    let state_type = vec![DataType::Utf8, DataType::UInt64]; // serialized form of map (flattened list later)

    create_udaf(
        "kmer_count",
        vec![DataType::Utf8],
        return_type,
        Volatility::Immutable,
        Arc::new(accumulator_creator),
        Arc::new(state_type),
    )
}

#[derive(Debug)]
struct KmerCountAccumulator {
    k: usize,
    counts: HashMap<String, u64>,
}

impl KmerCountAccumulator {
    fn new(k: usize) -> Self {
        Self {
            k,
            counts: HashMap::new(),
        }
    }

    fn canonical_kmer(kmer: &[u8]) -> Vec<u8> {
        let rc = kmer.iter().rev().map(|b| match b {
            b'A' | b'a' => b'T',
            b'T' | b't' => b'A',
            b'C' | b'c' => b'G',
            b'G' | b'g' => b'C',
            _ => b'N',
        }).collect::<Vec<u8>>();
        if rc.as_slice() < kmer {
            rc
        } else {
            kmer.to_vec()
        }
    }
}

impl Accumulator for KmerCountAccumulator {
    fn update_batch(&mut self, values: &[Arc<dyn Array>]) -> Result<()> {
        let array = values[0]
            .as_any()
            .downcast_ref::<StringArray>()
            .expect("Expected StringArray");

        for i in 0..array.len() {
            if array.is_null(i) {
                continue;
            }
            let seq = array.value(i).as_bytes();
            if seq.len() < self.k {
                continue;
            }
            for j in 0..=(seq.len() - self.k) {
                let window = &seq[j..j + self.k];
                if window.iter().all(|&b| matches!(b, b'A' | b'C' | b'G' | b'T' | b'a' | b'c' | b'g' | b't')) {
                    let canonical = Self::canonical_kmer(window);
                    let kmer_str = String::from_utf8_lossy(&canonical).to_string();
                    *self.counts.entry(kmer_str).or_insert(0) += 1;
                }
            }
        }

        Ok(())
    }

    fn merge_batch(&mut self, states: &[Arc<dyn Array>]) -> Result<()> {
        let kmers = states[0]
            .as_any()
            .downcast_ref::<StringArray>()
            .expect("Expected StringArray for kmer");

        let counts = states[1]
            .as_any()
            .downcast_ref::<UInt64Array>()
            .expect("Expected UInt64Array for count");

        for i in 0..kmers.len() {
            if kmers.is_null(i) || counts.is_null(i) {
                continue;
            }
            let kmer = kmers.value(i);
            let count = counts.value(i);
            *self.counts.entry(kmer.to_string()).or_insert(0) += count;
        }

        Ok(())
    }

    fn state(&mut self) -> Result<Vec<ScalarValue>> {
        Ok(vec![]) // nieużywane na razie
    }

    fn evaluate(&mut self) -> Result<ScalarValue> {
        let mut kmer_builder = StringBuilder::new();
        let mut count_builder = UInt64Builder::new();

        for (kmer, count) in &self.counts {
            kmer_builder.append_value(kmer);
            count_builder.append_value(*count);
        }

        let kmer_array = Arc::new(kmer_builder.finish());
        let count_array = Arc::new(count_builder.finish());

        let struct_array = StructArray::new(
            Fields::from(vec![
                Field::new("kmer", DataType::Utf8, false),
                Field::new("count", DataType::UInt64, false),
            ]),
            vec![kmer_array, count_array],
            None,
        );

        Ok(ScalarValue::Struct(Arc::new(struct_array)))
    }

    fn size(&self) -> usize {
        self.counts.len() * (std::mem::size_of::<String>() + std::mem::size_of::<u64>()) + std::mem::size_of::<Self>()
    }
}
