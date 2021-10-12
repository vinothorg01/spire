import datetime
from spire.targets import constants
from spire.utils.logger import get_logger
from spire.targets.aam_tables.create_aam_active_segments import (
    extract_spire_aam_segments,
    load_aam_segments_table,
    update_aam_segments,
    write_aam_segments_updates,
    create_aam_segments_table,
)

logger = get_logger()


def main(execution_date_str: str, **kwargs):
    # Add kwargs because the onenotebookjob injects run_date into main

    execution_date = datetime.datetime.strptime(execution_date_str, "%Y-%m-%d").date()
    active_aam_segments = extract_spire_aam_segments(execution_date)
    aam_segments_table = load_aam_segments_table()

    # Update the aam_segments table if it already exists.
    # Otherwise, create it.
    if aam_segments_table is not None:
        logger.info("Active segments table found, updating table...")
        aam_segs_updates = update_aam_segments(
            aam_segments_table, active_aam_segments, execution_date
        )
        write_aam_segments_updates(aam_segs_updates)
    else:
        logger.info("No active segments table found, creating one...")
        new_aam_segments_table = create_aam_segments_table(active_aam_segments)
        new_aam_segments_table.write.format("delta").save(
            constants.ACTIVE_SEGMENTS_TABLE_URI
        )

    logger.info("Finished write")


if __name__ == "__main__":
    main()
