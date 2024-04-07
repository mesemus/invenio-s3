import datetime

from flask import current_app


class LowLevelS3File:
    def __init__(self, fs, path, upload_id=None):
        self.fs = fs
        self.path = path
        self.bucket, self.key, self.version_id = fs.split_path(path)
        self.s3_client = fs.s3
        self.acl = fs.s3_additional_kwargs.get("ACL", "")
        self.upload_id = upload_id

    def create_multipart_upload(self):
        mpu = self.s3_client.create_multipart_upload(
            Bucket=self.bucket, Key=self.key, ACL=self.acl
        )
        # TODO: error handling here
        self.upload_id = mpu["UploadId"]
        return self.upload_id

    def get_parts(self, max_parts):
        ret = self.s3_client.list_parts(
            Bucket=self.bucket,
            Key=self.key,
            UploadId=self.upload_id,
            MaxParts=max_parts,
            PartNumberMarker=0,
        )
        return ret.get("Parts", [])

    def upload_part(self, part_number, data):
        part = self.s3_client.upload_part(
            Bucket=self.bucket,
            Key=self.key,
            UploadId=self.upload_id,
            PartNumber=part_number,
            Body=data,
        )
        return part

    def _filter_part_parameters(self, part):
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
        return self.s3_client.complete_multipart_upload(
            Bucket=self.bucket,
            Key=self.key,
            UploadId=self.upload_id,
            MultipartUpload={
                "Parts": [self._filter_part_parameters(part) for part in parts]
            },
        )

    def abort_multipart_upload(self):
        return self.s3_client.abort_multipart_upload(
            Bucket=self.bucket, Key=self.key, UploadId=self.upload_id
        )
