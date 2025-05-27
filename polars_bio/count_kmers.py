import polars as pl
from polars_bio.polars_bio import py_count_kmer_from_reader 
from .context import ctx

def count_kmers(
    df: pl.LazyFrame,
    k: int = 5,
    threads: int = 2
) -> pl.DataFrame:
    df = df.with_columns(pl.col("sequence").cast(pl.Utf8)).collect()
    ctx.set_option("datafusion.execution.target_partitions", f"{threads}")
    reader = df.to_arrow().to_reader()
    return py_count_kmer_from_reader(ctx, reader, k)
