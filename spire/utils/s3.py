import json
import boto3

from spire.utils.constants import AWS_RESPONSE_METADATA, AWS_RESPONSE_STATUS_CODE, OK


def assemble_path(bucket, key):
    return "s3a://{}/{}".format(bucket, key)


def get_messages_from_queue(queue_url):
    """Generates messages from an SQS queue.

    Note: this continues to generate messages until the queue is empty.
    Every message on the queue will be deleted.

    :param queue_url: URL of the SQS queue to drain.

    """
    sqs_client = boto3.client("sqs", region_name="us-east-1")

    while True:
        resp = sqs_client.receive_message(
            QueueUrl=queue_url, AttributeNames=["All"], MaxNumberOfMessages=10
        )

        try:
            yield from resp["Messages"]
        except KeyError:
            return

        entries = [
            {"Id": msg["MessageId"], "ReceiptHandle": msg["ReceiptHandle"]}
            for msg in resp["Messages"]
        ]

        resp = sqs_client.delete_message_batch(QueueUrl=queue_url, Entries=entries)

        if len(resp["Successful"]) != len(entries):
            raise RuntimeError(
                "Failed to delete : entries={} resp={}".format(entries, resp)
            )


def write_json(bucket, path, data):
    s3 = boto3.client("s3")
    response = s3.put_object(Body=json.dumps(data), Bucket=bucket, Key=path)
    if response[AWS_RESPONSE_METADATA][AWS_RESPONSE_STATUS_CODE] is not OK:
        raise RuntimeError(
            """AWS returned a non-200 status code
                           attempting to write JSON to S3 endpoint
                           {}/{}. Response returned was: \n {}
                           """.format(
                bucket, path, response
            )
        )


def read_json(bucket, path, encoding="utf-8", return_dict=True):
    s3 = boto3.resource("s3")
    try:
        s3_object = s3.Object(bucket, path)
        content = s3_object.get()["Body"].read().decode(encoding)
        if not return_dict:
            return content
        return json.loads(content)
    except Exception as e:
        raise e


def get_all_keys(bucket, prefix, generator=True):
    """Get a list of all keys in an S3 bucket."""
    s3 = boto3.client("s3")

    kwargs = {"Bucket": bucket, "Prefix": prefix}

    while True:
        resp = s3.list_objects_v2(**kwargs)
        for obj in resp["Contents"]:
            yield obj
        try:
            kwargs["ContinuationToken"] = resp["NextContinuationToken"]
        except KeyError:
            break
