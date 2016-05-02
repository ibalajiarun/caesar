#!/usr/bin/env python3

import boto3
import os.path
import sys


def upload(bucket, file):
    s3 = boto3.resource('s3')
    s3.Bucket(bucket).upload_file(file, os.path.basename(file),
                                  ExtraArgs={'ACL': 'public-read'})


if __name__ == '__main__':
    if len(sys.argv) < 3:
        print("usage: {0} bucket file")
        sys.exit(1)
    upload(sys.argv[1], sys.argv[2])
