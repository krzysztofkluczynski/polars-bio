import json
import polars as pl
import polars_bio as pb
from polars_bio.io import read_fastq
import pytest

def run_kmer_test(k: int, json_path: str):
    result = pb.sql(f"SELECT kmer_count(sequence, {k}) AS result FROM example").collect()
    result = result.unnest("result").sort("kmer")

    with open(json_path, "r") as f:
        expected_json = json.load(f)

    expected = pl.DataFrame(expected_json["values"]) \
        .rename({"k_mer": "kmer"}) \
        .select(["kmer", "count"]) \
        .sort("kmer")

    assert result.rows() == expected.rows(), f"Mismatch for k={k}"

def test_kmer_count_matches_expected():
    read_fastq("example.fastq")

    run_kmer_test(3, "fastqc-rs_output/fastqc_kmer3.json")
    run_kmer_test(5, "fastqc-rs_output/fastqc_kmer5.json")

def test_kmer_count_invalid_k_raises():
    read_fastq("example.fastq")  # rejestruje tabelę 'example'

    with pytest.raises(Exception) as exc_info:
        pb.sql("SELECT kmer_count(sequence, -2) AS result FROM example").collect()

    assert "k must be greater than 0" in str(exc_info.value)

def test_plot_kmer_counts_raises_on_top_n_too_high():
    read_fastq("example.fastq")
    result = pb.sql("SELECT kmer_count(sequence, 5) FROM example").collect()

    with pytest.raises(ValueError, match="must not exceed 100"):
        pb.plot_kmer_counts(result, top_n=101)
