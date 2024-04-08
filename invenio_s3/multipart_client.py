# -*- coding: utf-8 -*-
#
# Copyright (C) 2024 Miroslav Simek
#
# Invenio-S3 is free software; you can redistribute it and/or modify it
# under the terms of the MIT License; see LICENSE file for more details.


"""Low level client for S3 multipart uploads."""


import datetime

# WARNING: low-level code. The underlying s3fs currently does not have support
# for multipart uploads without keeping the S3File instance in memory between requests.
# To overcome this limitation, we have to use the low-level API directly separated in the
# LowLevelS3File class.


class MultipartS3File:
    """Low level client for S3 multipart uploads."""

    def __init__(self, fs, path, upload_id=None):
        """Initialize the low level client.

        :param fs: S3FS instance
        :param path: The path of the file (with bucket and version)
        :param upload_id: The upload ID of the multipart upload, can be none to get a new upload.
        """
        self.fs = fs
        self.path = path
        self.bucket, self.key, self.version_id = fs.split_path(path)
        self.s3_client = fs.s3
        self.acl = fs.s3_additional_kwargs.get("ACL", "")
        self.upload_id = upload_id

    def create_multipart_upload(self):
        """Create a new multipart upload.

        :returns: The upload ID of the multipart upload.
        """
        mpu = self.s3_client.create_multipart_upload(
            Bucket=self.bucket, Key=self.key, ACL=self.acl
        )
        # TODO: error handling here
        self.upload_id = mpu["UploadId"]
        return self.upload_id

    def get_parts(self, max_parts):
        """List the parts of the multipart upload.

        :param max_parts: The maximum number of parts to list.
        :returns: The list of parts, including checksums and etags.
        """
        ret = self.s3_client.list_parts(
            Bucket=self.bucket,
            Key=self.key,
            UploadId=self.upload_id,
            MaxParts=max_parts,
            PartNumberMarker=0,
        )
        return ret.get("Parts", [])

    def upload_part(self, part_number, data):
        """Upload a part of the multipart upload. Will be used only in tests.

        :param part_number: The part number.
        :param data: The data to upload.
        """
        part = self.s3_client.upload_part(
            Bucket=self.bucket,
            Key=self.key,
            UploadId=self.upload_id,
            PartNumber=part_number,
            Body=data,
        )
        return part

    def _complete_operation_part_parameters(self, part):
        """Filter parameters for the complete operation."""
        ret = {}
        for k in [
            "PartNumber",
            "ETag",
            "ChecksumCRC32",
            "ChecksumCRC32C",
            "ChecksumSHA1",
            "ChecksumSHA256",
        ]:
            if k in part:
                ret[k] = part[k]
        return ret

    def get_part_links(self, max_parts, url_expiration):
        """
        Generate pre-signed URLs for the parts of the multipart upload.

        :param max_parts: The maximum number of parts to list.
        :param url_expiration: The expiration time of the URLs in seconds

        :returns: The list of parts with pre-signed URLs and expiration times.
        """
        expiration = datetime.datetime.utcnow() + datetime.timedelta(
            seconds=url_expiration
        )
        expiration = expiration.replace(microsecond=0).isoformat() + "Z"

        return {
            "parts": [
                {
                    "part": part + 1,
                    "url": self.s3_client.generate_presigned_url(
                        "upload_part",
                        Params={
                            "Bucket": self.bucket,
                            "Key": self.key,
                            "UploadId": self.upload_id,
                            "PartNumber": part + 1,
                        },
                        ExpiresIn=url_expiration,
                    ),
                    "expiration": expiration,
                }
                for part in range(max_parts)
            ]
        }

    def complete_multipart_upload(self, parts):
        """Complete the multipart upload.

        :param parts: The list of parts (as from self.get_parts), including checksums and etags.
        """
        return self.s3_client.complete_multipart_upload(
            Bucket=self.bucket,
            Key=self.key,
            UploadId=self.upload_id,
            MultipartUpload={
                "Parts": [
                    self._complete_operation_part_parameters(part) for part in parts
                ]
            },
        )

    def abort_multipart_upload(self):
        """Abort the multipart upload."""
        return self.s3_client.abort_multipart_upload(
            Bucket=self.bucket, Key=self.key, UploadId=self.upload_id
        )
