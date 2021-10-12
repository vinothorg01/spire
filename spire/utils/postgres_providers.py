import uuid
import psycopg2
import json


def insert_segment(conn_str, name, categories):
    """Inserts a Spire segment into a Postgres table
    and assigns a primary key UUID

    Args:
        conn_str: string: auth information for
            a psycopg2 postgres connector
        name: string: a human-readable, unique spire segment name
    Returns:
        id: UUID4: the UUID for the segment just inserted
    """
    with psycopg2.connect(conn_str) as connection:
        cursor = connection.cursor()
        cursor.execute(
            """
        INSERT INTO segments
        (id, name, categories)
        VALUES (%s, %s, %s)
        RETURNING id
        """,
            (uuid.uuid4(), name, categories),
        )
    return cursor.fetchone()[0]


def query_postgres(conn_str, query):
    """Establishes a connection to the Spire
    database and executes a query

    Args:
        conn_str: string: auth information for
            a psycopg2 postgres connector
        query: string: query to run
    Returns:
        An array containing all data objects
         returned from the passed query
    """
    query_results = []
    with psycopg2.connect(conn_str) as connection:
        cursor = connection.cursor()
        cursor.execute(query)
        columns = [x[0] for x in cursor.description]
        for _tuple in cursor.fetchall():
            row = dict(zip(columns, _tuple))
            query_results.append(row)
    return query_results


def update_run_history(conn_str, table, response):
    """Helper function to save the outcome of a Spire process
    This should be invoked after training set assembly, training,
    and scoring. The outcome of the process will then be saved in
    the Spire database, where downstream steps can reference it.

    Args:
        conn_str: string: auth information for a psycopg2 postgres connector
        table: string: a constant variable specifying the table to be inserted
                options for spire are: assembly_history,
                                       training_history,
                                       scoring_history
        response: dict: each step of Spire returns a response object
                        containing the results of processing for each trait.
                        This function expects a dictionary with the following
                        key-value pairs:

                        {
                            trait_id: int,
                            status: string,
                            error: string or none,
                            traceback: string or none,
                            date: datetime.datetime or datetime.date
                            stats: json
                            warnings: json or none
                            info: json or none
                        }

                        The following fields are optional, as they aren't
                        included in some steps of Spire:

                        {
                            run_id: string,
                            time: int
                        }

    Returns:
        trait_id: int: the id of the just inserted trait
    """

    # required fields
    trait_id = response["trait_id"]
    status = response["status"]
    error = response["error"]
    traceback = response["traceback"]
    run_time = response["date"]

    # JSON fields
    stats = json.dumps(response["stats"])
    warnings = json.dumps(response["warnings"])
    info = json.dumps(response["info"])

    # optional fields
    mlflow_run_id = response.get("run_id", None)
    processing_time = response.get("time", None)

    with psycopg2.connect(conn_str) as connection:
        cursor = connection.cursor()
        cursor.execute(
            """
        INSERT INTO {}
        (trait_id, mlflow_run_id, run_time, stats, warnings,
        status, processing_time, error, traceback, info)
        VALUES
        (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        RETURNING trait_id
        """.format(
                table
            ),
            (
                trait_id,
                mlflow_run_id,
                run_time,
                stats,
                warnings,
                status,
                processing_time,
                error,
                traceback,
                info,
            ),
        )
