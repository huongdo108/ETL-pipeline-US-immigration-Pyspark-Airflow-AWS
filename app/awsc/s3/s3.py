import boto3
import configparser
import logging
from pathlib import Path
from botocore.exceptions import ClientError
import os


class S3Resource:
    def __init__(self, input_bucket, processing_bucket, processed_bucket, KEY, SECRET):
        self._s3 = boto3.resource("s3", region_name="eu-west-1", aws_access_key_id=KEY, aws_secret_access_key=SECRET)
        self._files = []
        self._input_bucket = input_bucket
        self._processing_bucket = processing_bucket
        self._processed_bucket = processed_bucket

    def upload_files(self, filepaths, bucket=None):
        """
        Upload a file to an S3 bucket
        :param file_name: File to upload
        :param bucket: Bucket to upload to
        :param object_name: S3 object name. If not specified then file_name is used
        :return: True if file was uploaded, else False
        """

        # If S3 object_name was not specified, use file_name
        # Upload the file
        if bucket is None:
            bucket = self._input_bucket
        try:
            for filepath in filepaths:
                print(f"upload file {filepath} to bucket {bucket}")
                object_name = os.path.basename(filepath)
                self._s3.meta.client.upload_file(filepath, bucket, object_name)
        except ClientError as e:
            logging.error(e)
            return False
        return True

    def get_files_in_bucket(self, s3_bucket):
        """
        Get all the files present in the s3_bucket
        :param s3_bucket: bucket to search
        :return: files in the bucket
        """
        logging.debug(f"Inspecting bucket: {s3_bucket} for files present")
        return [bucket_file.key for bucket_file in self._s3.Bucket(s3_bucket).objects.all()]

    def move_data(self, source_bucket=None, target_bucket=None):
        """
        Detect files in source bucket and move files from source_bucket to target_bucket
        
        """

        if source_bucket is None:
            source_bucket = self._input_bucket
        if target_bucket is None:
            target_bucket = self._processing_bucket

        logging.debug(f"s3_move_data : source bucket is : {source_bucket}\n target bucket is : {target_bucket}")

        # cleanup target bucket
        self.clean_s3_bucket(target_bucket)

        # Move files to processing bucket
        for file in self.get_files_in_bucket(source_bucket):
            logging.debug(f"Copying file {file} from {source_bucket} to {target_bucket}")
            self._s3.meta.client.copy({"Bucket": source_bucket, "Key": file}, target_bucket, file)

        # cleanup source bucket
        # self.clean_bucket(source_bucket)

    def clean_s3_bucket(self, s3_bucket):
        """
        Delete all files in s3_bucket
        :param s3_bucket: bucket to clean
        """
        logging.debug(f"Cleaning bucket: {s3_bucket}")
        self._s3.Bucket(s3_bucket).objects.all().delete()
