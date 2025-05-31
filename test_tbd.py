import polars_bio as pb
import pandas as pd
from polars_bio.io import read_fastq
import polars as pl

df = read_fastq("example.fastq")

print(df)
print(df.schema)
pl.Config.set_tbl_rows(100)
print(pb.sql("SELECT kmer_count(sequence, 1) AS result FROM example LIMIT 60").collect())


#counts = pb.count_kmers(df, k=5, threads=8)
#pb.plot_kmer_counts(counts, top_n=30, filepath="kmers.png")
#print(counts)
