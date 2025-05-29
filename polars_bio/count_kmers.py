import polars as pl
import pandas as pd
import pyarrow as pa
from typing import Union, Literal
from polars_bio.polars_bio import py_count_kmer_from_reader
from .context import ctx

import matplotlib.pyplot as plt

def plot_kmer_counts(df: pl.DataFrame, top_n: int = 20, filepath: str | None = None):
    """
    Visualize the top N most frequent k-mers as a bar chart.

    Parameters:
    - df: pl.DataFrame with columns "kmer" and "count"
    - top_n: Number of top k-mers to display
    - filepath: Optional path to save the plot. If None, shows interactively.
    """
    if "kmer" not in df.columns or "count" not in df.columns:
        raise ValueError("DataFrame must contain 'kmer' and 'count' columns.")
    
    # Sort and extract top N
    sorted_df = df.sort("count", descending=True).head(top_n)
    
    kmers = sorted_df["kmer"].to_list()
    counts = sorted_df["count"].to_list()

    plt.figure(figsize=(12, 6))
    plt.bar(kmers, counts)
    plt.xticks(rotation=90)
    plt.xlabel("k-mer")
    plt.ylabel("Count")
    plt.title(f"Top {top_n} k-mers")
    plt.tight_layout()

    if filepath:
        plt.savefig(filepath, dpi=300)
        plt.close()
    else:
        plt.show()




def count_kmers(
    df: pl.LazyFrame,
    k: int = 5,
    threads: int = 2
) -> pl.DataFrame:
    df = df.with_columns(pl.col("sequence").cast(pl.Utf8)).collect()
    ctx.set_option("datafusion.execution.target_partitions", f"{threads}")
    reader = df.to_arrow().to_reader()
    return py_count_kmer_from_reader(ctx, reader, k)
