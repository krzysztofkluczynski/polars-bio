use needletail::{parse_fastx_file, Sequence};
use rustc_hash::FxHashMap;

pub fn count_kmers_from_fastq(path: &str, k: u8) -> FxHashMap<Vec<u8>, u64> {
    let mut kmers = FxHashMap::default();
    let mut reader = parse_fastx_file(path).expect("Cannot open FASTQ file");
    println!("Reading FASTQ file: {}", path);
    println!("Counting kmers of size {}...", k);
    println!("This may take a while...");
    println!("Press Ctrl+C to cancel.");
    println!("Counting kmers...");
    println!("This may take a while...");

    while let Some(record) = reader.next() {
        let seqrec = record.expect("Invalid FASTQ record");
        let norm_seq = seqrec.normalize(false);
        let rc = norm_seq.reverse_complement();

        for (_, kmer, _) in norm_seq.canonical_kmers(k, &rc) {
            *kmers.entry(kmer.to_vec()).or_insert(0) += 1;
        }
    }

    kmers
}
