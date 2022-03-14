# -*- coding: utf-8 -*-
import logging
import os
import subprocess
from pathlib import Path

import click
import pendulum
import requests
import yaml
from dotenv import find_dotenv, load_dotenv

from falcon.common import CURR_UNIXTIME, setup_logging
from falcon.utils.s3fs_utils import S3FSActions
from falcon.utils.vault_utils import VaultAccess


def run_query(query):
    """Runs the graphql query to extract the deserializer schema for Protobuf serialized streaming data.
    A simple function to use requests.post to make the API call. Note the json= section.

    Args:
        query:  Query to be passed for graphql

    Returns: Json formatted schema for query

    """
    headers = {}
    request = requests.post(
        "https://mr.conde.io/graphql", json={"query": query}, headers=headers
    )
    if request.status_code == 200:
        return request.json()
    else:
        raise Exception(
            f"Query failed to run by returning code of {request.status_code}. {query}"
        )


@click.command()
@click.option(
    "--schema",
    default=False,
    is_flag=True,
    help="Update protobuf schemas using this boolean flag",
)
@click.option(
    "--sync_to_s3",
    default=False,
    is_flag=True,
    help="Sync code data and models to s3 bucket",
)
@click.option(
    "--sync_from_s3",
    default=False,
    is_flag=True,
    help="Sync code data and models from s3 bucket",
)
@click.option("--mode", type=click.Choice(["dev", "prod"]))
@click.argument("settings_file", type=click.Path(exists=True))
def main(schema, sync_to_s3, sync_from_s3, mode, settings_file):
    """
    """

    logger = logging.getLogger(__name__)
    vt_aws_data = VaultAccess(mode=mode).get_settings(settings_type="aws")
    s3fs = S3FSActions(
        key=vt_aws_data.get("access_key_id"),
        secret=vt_aws_data.get("secret_access_key"),
    )

    with open(settings_file, "rt") as f:
        config = yaml.safe_load(f.read())

    if sync_to_s3:
        logger.info("Sync Data to S3 bucket !!!")
        project_bucket = config["project_bucket"]["s3_bucket"][mode]
        subprocess.call(
            f"AWS_ACCESS_KEY_ID={vt_aws_data['access_key_id']} "
            f"AWS_SECRET_ACCESS_KEY={vt_aws_data['secret_access_key']} "
            f"AWS_DEFAULT_REGION=us-west-2 aws s3 "
            f"sync {os.path.join(project_dir, 'data')} {os.path.join(project_bucket, 'codedata')}",
            shell=True,
        )

        subprocess.call(
            f"AWS_ACCESS_KEY_ID={vt_aws_data['access_key_id']} "
            f"AWS_SECRET_ACCESS_KEY={vt_aws_data['secret_access_key']} "
            f"AWS_DEFAULT_REGION=us-west-2 aws s3 "
            f"sync {os.path.join(project_dir, 'models')} {os.path.join(project_bucket, 'models')}",
            shell=True,
        )

    if sync_from_s3:
        logger.info("Sync Data from S3 bucket !!!")
        project_bucket = config["project_bucket"]["s3_bucket"][mode]
        subprocess.call(
            f"AWS_ACCESS_KEY_ID={vt_aws_data['access_key_id']} "
            f"AWS_SECRET_ACCESS_KEY={vt_aws_data['secret_access_key']} "
            f"AWS_DEFAULT_REGION=us-west-2 aws s3 "
            f"sync {os.path.join(project_bucket, 'codedata')} {os.path.join(project_dir, 'data')}",
            shell=True,
        )

        subprocess.call(
            f"AWS_ACCESS_KEY_ID={vt_aws_data['access_key_id']} "
            f"AWS_SECRET_ACCESS_KEY={vt_aws_data['secret_access_key']} "
            f"AWS_DEFAULT_REGION=us-west-2 aws s3 "
            f"sync {os.path.join(project_bucket, 'models')} {os.path.join(project_dir, 'models')}",
            shell=True,
        )

    if settings_file and schema:
        if schema:  # updated protobuf deserializing schemas here
            logger.info(
                f"Updated Schemas at {pendulum.from_timestamp(int(CURR_UNIXTIME)).to_datetime_string()}"
            )

            query = """
            query {{
              findModelByName(name: "{traffic_topic}") {{
                kafkaTopic
                serializerDeserializer {{
                  schema
                }}
              }}
            }}
            """
            for keys in config["protobuf_schema"]["deserializer_script"].keys():
                variables = {"traffic_topic": keys}
                result = run_query(query.format(**variables))

                with open(f"{keys}.proto", "w") as f:
                    f.write(
                        result["data"]["findModelByName"]["serializerDeserializer"][
                            "schema"
                        ]
                    )

                # After this is created run the following shell commands
                subprocess.call(
                    f"protoc --proto_path=. --python_out=. ./{keys}.proto", shell=True
                )

                # After that sync the sparrow_pb2.py code to falcon repo
                s3fs.write_file_to_s3(
                    f"{keys}_pb2.py",
                    config["protobuf_schema"]["deserializer_script"][keys]["s3_bucket"][
                        mode
                    ],
                )

                subprocess.call(f"rm {keys}.proto {keys}_pb2.py", shell=True)

            schema_protobuf_bucket = config["settings"]["protobuf_schema"]["s3_bucket"][
                mode
            ]
            logger.info(
                f"Schemas Updated for Kafka Streams at {schema_protobuf_bucket}!!!"
            )
    else:
        raise Exception(
            "Settings files should be provided with schema flag to update the protobuf schemas in S3 location!!!"
        )


if __name__ == "__main__":
    setup_logging()

    # not used in this stub but often useful for finding various files
    project_dir = Path(__file__).resolve().parents[1]
    # print(project_dir)

    # find .env automagically by walking up directories until it's found, then
    # load up the .env entries as environment variables
    load_dotenv(find_dotenv())

    main()  # pylint: disable=no-value-for-parameter
