import click
import datetime
from spire.utils.logger import get_logger
from spire.targets import (
    TargetsRunner,
    AdobeConfig,
    DFPClicksConfig,
    DFPImpressionsConfig,
    CDSIndividualConfig,
    CDSIndividualDemoConfig,
    NCSCustomConfig,
    NCSSyndicatedConfig,
    DARConfig,
    CondeNastConfig,
    CustomConfig,
)

# Note: Custom Targets have variable sources due to their custom nature.
# Hence, these targets are processed as a batch for all vendor=custom,
# irrespective of the source.
TARGET_CONFIG_MAP = {
    "adobe": {"mcid": AdobeConfig},
    "dfp": {
        "network_clicks": DFPClicksConfig,
        "network_impressions": DFPImpressionsConfig,
    },
    "ncs": {
        "custom": NCSCustomConfig,
        "syndicated": NCSSyndicatedConfig,
    },
    "cds": {
        "alt_individual": CDSIndividualConfig,
        "alt_individual_demo": CDSIndividualDemoConfig,
    },
    "nielsen": {"dar": DARConfig},
    "condenast": {"ga": CondeNastConfig},
    "custom": {None: CustomConfig},
}


def main(vendor: str, source: str, execution_date: str, **kwargs):
    # kwargs are needed because additional default parameters could be passed in
    execution_datetime = datetime.datetime.strptime(execution_date, "%Y-%m-%d").date()
    # The execution_date already has a 1 day lag built in so subtracting 1 day
    # from this date creates a 2 day lag for the targets. This lag allows for all
    # dependent datasets to be available.
    # The exception to this is Adobe which requires a 1 day lag (hence subtract 0 days)
    if vendor == "adobe":
        days_lag = 0
    else:
        days_lag = 1
    run_date = execution_datetime - datetime.timedelta(days=days_lag)
    logger = get_logger()
    logger.info(f"Targets are being processed for {run_date}")
    vendor_config = TARGET_CONFIG_MAP[vendor][source](run_date)
    runner = TargetsRunner(vendor_config)
    runner.load_transform_write()


# click.command doesn't play nicely with sys.modules, so we have to separate
# them.
@click.command()
@click.option("--vendor")
@click.option("--source")
@click.option("--execution_date")
def cli_main(vendor: str = None, source: str = None, execution_date: str = None):
    main(vendor, source, execution_date)


if __name__ == "__main__":
    cli_main()
