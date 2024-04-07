import pytest

MB = 2**20


def test_multipart_flow(base_app, s3fs):

    part_size = 7 * MB
    last_part_size = 5 * MB

    # initialize the upload
    upload_metadata = dict(
        parts=2, part_size=part_size, size=part_size + last_part_size
    )
    upload_metadata |= s3fs.multipart_initialize_upload(**upload_metadata) or {}

    # can not commit just now because no parts were uploaded
    with pytest.raises(ValueError):
        s3fs.multipart_commit_upload(**upload_metadata)

    # check that links are generated

    links = s3fs.multipart_links(**upload_metadata)["parts"]
    assert len(links) == 2
    assert links[0]["part"] == 1
    assert "url" in links[0]
    assert links[1]["part"] == 2
    assert "url" in links[1]

    # upload the first part manually
    low_level_file = s3fs.low_level_file(upload_metadata["uploadId"])
    low_level_file.upload_part(1, b"0" * part_size)
    assert len(low_level_file.get_parts(2)) == 1

    # still can not commit because not all parts were uploaded
    with pytest.raises(ValueError):
        s3fs.multipart_commit_upload(**upload_metadata)

    # upload the second part
    low_level_file.upload_part(2, b"1" * last_part_size)
    assert len(low_level_file.get_parts(2)) == 2

    s3fs.multipart_commit_upload(**upload_metadata)

    assert s3fs.open("rb").read() == b"0" * part_size + b"1" * last_part_size


def test_multipart_abort(base_app, s3fs):
    part_size = 7 * MB
    last_part_size = 5 * MB

    # initialize the upload
    upload_metadata = dict(
        parts=2, part_size=part_size, size=part_size + last_part_size
    )
    upload_metadata |= s3fs.multipart_initialize_upload(**upload_metadata) or {}

    s3fs.multipart_abort_upload(**upload_metadata)


def test_set_content_not_supported(base_app, s3fs):
    part_size = 7 * MB
    last_part_size = 5 * MB

    # initialize the upload
    upload_metadata = dict(
        parts=2, part_size=part_size, size=part_size + last_part_size
    )
    upload_metadata |= s3fs.multipart_initialize_upload(**upload_metadata) or {}

    with pytest.raises(NotImplementedError):
        s3fs.multipart_set_content(1, b"0" * part_size, part_size, **upload_metadata)
