import datetime

AAM_INFO_FILE_FIXTURE_PATH = (
    "tests/targets/fixtures/S3_77904_1955_iter_1612500023000.info"
)

MCID_TABLE_REF_DATE = datetime.date(2021, 2, 1)

AAM_KEYS = [
    {
        "Key": "adobe/aamsegsmcid/2018-10-17/S3_77904_1955_full_1539869790000.info",
        "LastModified": datetime.datetime(2019, 1, 2),
        "ETag": '"3bc2025a96c6825369bb4a2aa1f8ee81"',
        "Size": 585,
        "StorageClass": "STANDARD",
    },
    {
        "Key": "adobe/aamsegsmcid/2021-02-01/S3_77904_1955_iter_1612500023000.info",
        "LastModified": datetime.datetime(2021, 2, 4),
        "ETag": '"3bc2025a96c6825369bb4a2aa1f8ee81"',
        "Size": 585,
        "StorageClass": "STANDARD",
    },
    {
        "Key": "adobe/aamsegsmcid/2018-10-17/S3_77904_1955_full_1521839790000.info",
        "LastModified": datetime.datetime(2019, 1, 2),
        "ETag": '"3bc2025a96c6825369bb4a2aa1f8ee81"',
        "Size": 585,
        "StorageClass": "STANDARD",
    },
    {
        "Key": "adobe/aamsegsmcid/2018-10-10/S3_77904_1955_iter_1539869790000.sync",
        "LastModified": datetime.datetime(2019, 1, 3),
        "ETag": '"6e97cfc43834bbeff178721121f69da0"',
        "Size": 585,
        "StorageClass": "STANDARD",
    },
    {
        "Key": "adobe/aamsegsmcid/2018-10-10/S3_77904_1955_iter_1539869890000.sync",
        "LastModified": datetime.datetime(2019, 1, 3),
        "ETag": '"6e97cfc43834bbeff178721121f69da0"',
        "Size": 585,
        "StorageClass": "STANDARD",
    },
    {
        "Key": "adobe/aamsegsmcid/2018-10-10/S3_77904_1955_iter_1539869890000.info",
        "LastModified": datetime.datetime(2019, 1, 3),
        "ETag": '"6e97cfc43834bbeff178721121f69da0"',
        "Size": 585,
        "StorageClass": "STANDARD",
    },
]

AAM_INFO_FILES = {
    "S3_77904_1955_full_1539869790000.info": "s3a://cn-data-vendor/adobe/aamsegsmcid/"
    + "2018-10-17/S3_77904_1955_full_1539869790000.info",
    "S3_77904_1955_full_1521839790000.info": "s3a://cn-data-vendor/adobe/aamsegsmcid/"
    + "2018-10-17/S3_77904_1955_full_1521839790000.info",
    "S3_77904_1955_iter_1539869890000.info": "s3a://cn-data-vendor/adobe/aamsegsmcid/"
    + "2018-10-10/S3_77904_1955_iter_1539869890000.info",
    "S3_77904_1955_iter_1612500023000.info": "s3a://cn-data-vendor/adobe/aamsegsmcid/"
    + "2021-02-01/S3_77904_1955_iter_1612500023000.info",
}
AAM_SYNC_FILES = {
    "S3_77904_1955_iter_1539869790000.sync": "s3a://cn-data-vendor/adobe/aamsegsmcid/"
    + "2018-10-10/S3_77904_1955_iter_1539869790000.sync",
    "S3_77904_1955_iter_1539869890000.sync": "s3a://cn-data-vendor/adobe/aamsegsmcid/"
    + "2018-10-10/S3_77904_1955_iter_1539869890000.sync",
}

AAM_INFO_FILES_DICT = {
    "S3_77904_1955_full_1539869790000.info": {
        "type": "full",
        "timestamp": 1539869790,
        "date": datetime.date(2018, 10, 18),
        "uri": "s3a://cn-data-vendor/adobe/aamsegsmcid/"
        + "2018-10-17/S3_77904_1955_full_1539869790000.info",
    },
    "S3_77904_1955_iter_1612500023000.info": {
        "type": "iter",
        "timestamp": 1612500023,
        "date": datetime.date(2021, 2, 4),
        "uri": "s3a://cn-data-vendor/adobe/aamsegsmcid/"
        + "2021-02-01/S3_77904_1955_iter_1612500023000.info",
    },
    "S3_77904_1955_iter_1539869890000.info": {
        "type": "iter",
        "timestamp": 1539869890,
        "date": datetime.date(2018, 10, 18),
        "uri": "s3a://cn-data-vendor/adobe/aamsegsmcid/"
        + "2018-10-10/S3_77904_1955_iter_1539869890000.info",
    },
    "S3_77904_1955_full_1521839790000.info": {
        "type": "full",
        "timestamp": 1521839790,
        "date": datetime.date(2018, 3, 23),
        "uri": "s3a://cn-data-vendor/adobe/aamsegsmcid/"
        + "2018-10-17/S3_77904_1955_full_1521839790000.info",
    },
}

ITER_1612500023000_INFO_FILE_DICT = {
    "type": "iter",
    "timestamp": 1612500023,
    "date": datetime.date(2021, 2, 4),
    "uri": AAM_INFO_FILE_FIXTURE_PATH,
    "ref_date": MCID_TABLE_REF_DATE,
}


AAM_RUNTHROUGH_KEYS = [
    {
        "Key": "targets/fixtures/S3_77904_1955_iter_1612500023000.sync",
        "LastModified": datetime.datetime(2021, 2, 4),
        "ETag": '"3bc2025a96c6825369bb4a2aa1f8ee81"',
        "Size": 585,
        "StorageClass": "STANDARD",
    },
    {
        "Key": "targets/fixtures/S3_77904_1955_iter_1612500023000.info",
        "LastModified": datetime.datetime(2021, 2, 4),
        "ETag": '"3bc2025a96c6825369bb4a2aa1f8ee81"',
        "Size": 585,
        "StorageClass": "STANDARD",
    },
]
