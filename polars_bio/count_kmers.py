import polars as pl
import pandas as pd
import pyarrow as pa
from typing import Union, Literal
from polars_bio.polars_bio import py_count_kmer_from_reader
from .context import ctx

import matplotlib.pyplot as plt

import matplotlib.pyplot as plt
import polars as pl

def plot_kmer_counts(sql_result: pl.DataFrame, top_n: int = 20, filepath: str | None = None):
    """
    Takes a SQL result, extracts fields,
    aggregates counts, and plots a horizontal bar chart with k-mers on the y-axis.
    Dynamically adjusts height and adds labels with exact counts.
    """
    if top_n > 100:
        raise ValueError("Parameter 'top_n' must not exceed 100.")

    # Find the first struct column
    struct_col = next(
        (name for name, dtype in sql_result.schema.items() if isinstance(dtype, pl.Struct)),
        None
    )

    if struct_col is None:
        raise ValueError("No struct column found in the SQL result. Expected a column with kmer/count struct.")

    # Extract fields from struct
    unnested = sql_result.select([
        pl.col(struct_col).struct.field("kmer"),
        pl.col(struct_col).struct.field("count")
    ])

    # Aggregate and sort
    aggregated = unnested.group_by("kmer").agg(pl.sum("count").alias("count"))
    sorted_df = aggregated.sort("count", descending=True).head(top_n)

    kmers = sorted_df["kmer"].to_list()
    counts = sorted_df["count"].to_list()

    # Dynamic figure height
    height_per_bar = 0.4
    fig_height = max(4, len(kmers) * height_per_bar)

    # Adaptive margin based on top_n
    y_margin = 0.000333 * top_n + 0.01667
    y_margin = min(max(y_margin, 0.01), 0.06)

    # Plot
    plt.figure(figsize=(10, fig_height))
    bars = plt.barh(kmers, counts)
    plt.xlabel("Count")
    plt.ylabel("k-mer")
    plt.title(f"Top {min(top_n, len(sorted_df))} k-mers (of {aggregated.height} total)")
    plt.gca().invert_yaxis()
    plt.gca().margins(y=y_margin)
    plt.tight_layout()

    # Add text labels next to bars
    for bar, count in zip(bars, counts):
        width = bar.get_width()
        plt.text(width + max(counts) * 0.01, bar.get_y() + bar.get_height() / 2,
                 f"{count}", va='center', fontsize=9)

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
