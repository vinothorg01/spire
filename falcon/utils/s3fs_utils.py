import pickle

import os
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import s3fs


class S3FSActions:
    def __init__(self, key, secret):
        self.s3 = s3fs.S3FileSystem(key=key, secret=secret)

    def delete_s3_path(self, file_path):
        """

        Args:
            file_path:

        Returns:

        """
        if self.s3.exists(file_path):
            self.s3.rm(file_path)
            print(f"Deleted already existing {file_path}")

    def delete_s3_folder(self, folder_path):
        """

        Args:
            folder_path:

        Returns:

        """
        if self.s3.exists(folder_path):
            self.s3.rm(folder_path, recursive=True)
            print(f"Deleted already existing {folder_path}")

    def write_file_to_s3(self, localpath, writepath):
        """

        Args:
            localpath: location of a local file
            writepath: location of the s3 file

        Returns:

        """
        with open(localpath, "rb") as f:
            bytes_to_write = f.read()
        with self.s3.open(writepath, "wb", acl="bucket-owner-full-control") as f:
            f.write(bytes_to_write)

    def read_csv_from_s3(self, s3_location):
        """

        Args:
            s3_location:

        Returns:

        """
        with self.s3.open(s3_location) as f:
            df = pd.read_csv(f)
        return df

    def write_csv_to_s3(self, df, write_location):
        """
        Code to write a csv file or basically a dataframe as a csv file to an S3 bucket
        Args:
            df: dataframe to be written
            write_location: This parameter should begin with /, for example /data/content_metadata.csv

        Returns: This function does not return anything

        """
        bytes_to_write = df.to_csv(None).encode()
        with self.s3.open(write_location, "wb", acl="bucket-owner-full-control") as f:
            f.write(bytes_to_write)

    def write_parquet_to_s3(self, df, **kwargs):
        """

        Args:
            df: dataframe
            kwargs: contains schemaloction, s3bucket, filesystem and partition_cols

        Returns:

        """
        s3bucket = kwargs.get("s3bucket")
        filesystem = kwargs.get("fs", self.s3)
        partition_cols = kwargs.get("partition_cols", None)
        schemalocation = kwargs.get("schemalocation")

        if not os.path.exists(schemalocation):
            print("Schema does not exist, creating the schemas in model folder")
            table = pa.Table.from_pandas(df=df, preserve_index=False)
            schema = table.schema
            with open(schemalocation, "wb",) as handle:
                pickle.dump(schema, handle)
        else:
            print("Schemas exist and enforcing the schema")
            with open(schemalocation, "rb",) as handle:
                schema = pickle.load(handle)
            table = pa.Table.from_pandas(df=df, schema=schema, preserve_index=False)

        pq.write_to_dataset(
            table,
            s3bucket,
            filesystem=filesystem,
            compression="gzip",
            partition_cols=partition_cols,
            flavor={"spark"},
            version="2.0",
        )
