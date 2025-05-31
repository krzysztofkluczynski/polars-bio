import polars_bio as pb
import pandas as pd
from polars_bio.io import read_fastq
import polars as pl

df = read_fastq("example.fastq")

print(df)
print(df.schema)
pl.Config.set_tbl_rows(100)
sql_result = pb.sql("SELECT kmer_count(sequence, 5) AS result FROM example LIMIT 60").collect()
print(sql_result)

pb.plot_kmer_counts(sql_result, top_n=30, filepath="kmers.png")


#counts = pb.count_kmers(df, k=5, threads=8)
#pb.plot_kmer_counts(counts, top_n=30, filepath="kmers.png")
#print(counts)
