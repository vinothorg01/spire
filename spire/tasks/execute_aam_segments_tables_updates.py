from spire.utils.logger import get_logger
from spire.targets.aam_tables.update_aam_segments_tables import (
    check_aam_targets_write,
    load_mcid_segments_table,
    get_latest_aam_file_date,
    update_active_segments,
    write_active_segments_update,
)

logger = get_logger()


def main(execution_date_str: str, **kwargs):

    # Update active segments table
    targets_exist = check_aam_targets_write(execution_date_str)
    if targets_exist:
        logger.info("Targets exist, proceeding with active segments update...")
        mcid_segments = load_mcid_segments_table()
        last_processed_date = get_latest_aam_file_date(mcid_segments)

        logger.info("Updating active segments last processed date...")
        active_segments = update_active_segments(last_processed_date)
        write_active_segments_update(active_segments)
    else:
        raise Exception(f"AAM targets were not written for {execution_date_str}")


if __name__ == "__main__":
    main()
