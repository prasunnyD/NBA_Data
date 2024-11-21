import pandas as pd
import duckdb
import polars as pl
# import boto3
# import logging
# import gzip
# from io import StringIO, BytesIO
# import json
# import logging
# import snappy
# import zstandard
# from urllib import parse

def mergeTables(df1 : pd.DataFrame, df2 : pd.DataFrame) -> pd.DataFrame:
    """
    Merges two dataframes into one
    """
    result = pd.merge(df1, df2)
    return result

class Database:
    def __init__(self, db_name : str):
        self.db_name = db_name
        self.db = duckdb.connect(db_name)

    def query(self, query : str):
        return self.db.query(query)
    
    def create_table(self, table_name : str, df : pl.DataFrame):
        self.db.register(table_name, df)

# class S3Sink:
#     def __init__(self, bucket) -> None:
#         self.s3 = boto3.resource("s3")
#         self.bucket=bucket

#     def file_extention(self):
#         supported_compression = {"gzip": ".gz", }
#     def file_path(self, source_system : str ,file : str):
#         file_ext= self.file_extention()
#         return f"prasun-nba-model/{source_system}/{file}"
    
#     def write_json_from_pd(self,source_system : str, model : str, df : pd.DataFrame, compress : str = "snappy"):
#         records = df.to_json(orient="records", lines=True).split("\n")

#         bucket = self.s3.Bucket(self.bucket_name)
#         if not self.check_if_bucket_exists():
#             bucket.create()
#         buffer = BytesIO()
#         if compress == "gzip":
#             with gzip.open(buffer, "wb") as f:
#                 for record in records:
#                     f.write(record+"\n").encode("utf-8")
#         elif compress == "zstd":
#             with zstandard.open(buffer, "wb") as f:
#                 for record in records:
#                     f.write(record+"\n").encode("utf-8")
#         elif compress == "snappy":
#             snappy_buffer= BytesIO()
#             for record in records:
#                 snappy_buffer.write((records+"\n").encode("utf-8"))
#             snappy_buffer.seek(0)
#             snappy.hadoop_snappy.stream_compress(snappy_buffer, buffer)
#         else:
#             for record in records:
#                 buffer.write((record + "\n").encode("utf-8"))

#         file_key = self.file_path(source_system,file)
#         metadata={}
#         metadata.update(self.metadata)
#         tags = parse.urlencode(metadata)

#         self.s3.Object(self.bucket, file_key).put(Body=buffer.getvalue(), Metadata=metadata, Tagging=tags)
            

#     def read_json(self):
#         obj = self.s3.Object(self.bucket_name, file_key)
#         logging.info(f"Reading fiel {file_key}")