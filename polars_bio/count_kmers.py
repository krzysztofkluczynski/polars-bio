import polars as pl
from polars_bio.polars_bio import py_count_kmer_from_reader 
from .context import ctx

def count_kmers(
    df: pl.LazyFrame,
    k: int = 5
) -> pl.DataFrame:
    df = df.with_columns(pl.col("sequence").cast(pl.Utf8)).collect()
    print(df.to_arrow().schema)  # debug: upewnij się, że to jest 'string'
    reader = df.to_arrow().to_reader()
    return py_count_kmer_from_reader(ctx, reader, k)
