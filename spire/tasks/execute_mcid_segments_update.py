from spire.utils.logger import get_logger
from spire.targets.aam_tables.create_aam_mcid_segments import (
    load_mcid_segments,
    get_info_files_to_process,
    get_latest_ref_date,
    load_and_process_aam_segments,
    write_mcid_segments,
)

logger = get_logger()


def main(**kwargs):
    # Add kwargs because the onenotebookjob injects run_date into main

    mcid_segments = load_mcid_segments()
    logger.info("Getting info files to process...")
    info_files_to_process, aam_info_file_dict = get_info_files_to_process(mcid_segments)

    for info_file in info_files_to_process:
        # Load aam table with every iteration as updates are made based on
        # the most recent version of the table
        mcid_segments = load_mcid_segments()
        info_file_dict = aam_info_file_dict[info_file]
        if (mcid_segments is None) & (info_file_dict["type"] == "iter"):
            raise Exception(
                "AAM table not found. Try again when next full file is available."
            )
        updated_info_file_dict = get_latest_ref_date(info_file_dict, mcid_segments)
        mcid_segments_update = load_and_process_aam_segments(updated_info_file_dict)
        write_mcid_segments(mcid_segments_update, updated_info_file_dict)
        logger.info(f"Write complete for {info_file}")
    logger.info(f"Wrote all info files found. Total: {len(info_files_to_process)}")


if __name__ == "__main__":
    main()
