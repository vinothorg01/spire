import logging
from pathlib import Path
import click
import pendulum
from dotenv import find_dotenv, load_dotenv
from falcon.common import CURR_UNIXTIME
from falcon.database.sqlalchemy_utils import SQLAlchemyUtils
from falcon.utils.vault_utils import VaultAccess
from sqlalchemy import (
    BigInteger,
    Boolean,
    Column,
    Float,
    Integer,
    MetaData,
    String,
    Table,
    UniqueConstraint,
    ForeignKey,
)
from sqlalchemy.dialects.postgresql import UUID
from sqlalchemy.schema import CreateSchema

# TODO : Add the other columns in the migrations to the databases \
# TODO : and also figure out the best way to remove tables not required


def create_common_dbs(metadata, engine):
    """
    This section creates common dbs accessed by all brand dbs
    """
    # metadata is used for the relationships
    social_accounts = Table(
        "social_accounts",
        metadata,
        Column("id", UUID(as_uuid=True), primary_key=True),
        Column("account_type", String(), nullable=False),
        schema="public",
    )
    metadata.create_all(bind=engine, tables=[social_accounts], checkfirst=True)

    # metadata is used for the relationships
    socialflow_accounts = Table(
        "socialflow_accounts",
        metadata,
        Column("social_account_type", String(), primary_key=True),
        Column("brand_name", String(), primary_key=True),
        Column("client_service_id", String(), nullable=False),
        Column("service_user_id", String(), nullable=False),
        Column("client_id", String(), nullable=False),
        Column("client_name", String(), primary_key=True),
        schema="public",
    )
    metadata.create_all(bind=engine, tables=[socialflow_accounts], checkfirst=True)


def create_brand_dbs(
    metadata, metadata_public, engine, social_account, brand_name, mode
):

    """
    This section is to delete tables if required
    """
    # # If required to delete a table, reflect and drop it... BE CAREFUL !!!
    # try:
    #     # content_last_update_time = Table("content_last_update_time", metadata, autoload=True, autoload_with=engine)
    #     # falcon_later = Table("falcon_later", metadata, autoload=True, autoload_with=engine)
    #     # falcon_never = Table("falcon_never", metadata, autoload=True, autoload_with=engine)
    #     model_outcomes = Table(
    #         "model_outcomes", metadata, autoload=True, autoload_with=engine
    #     )
    #     socialflow_queue_posted_data = Table(
    #         "socialflow_queue_posted_data",
    #         metadata,
    #         autoload=True,
    #         autoload_with=engine,
    #     )
    #     model_outcomes.drop(bind=engine)
    #     socialflow_queue_posted_data.drop(bind=engine)
    # except Exception as e:
    #     print(f"Tables might have already been dropped, {e}")

    """
    Following section creates tables
    """

    socialcopy_last_update_time = Table(
        "socialcopy_last_update_time",
        metadata_public,
        Column(
            "social_account_id",
            UUID(as_uuid=True),
            ForeignKey("social_accounts.id"),
            nullable=False,
        ),
        Column("ts_epoch", BigInteger(), nullable=False),
        schema=brand_name,
    )
    metadata.create_all(
        bind=engine, tables=[socialcopy_last_update_time], checkfirst=True
    )

    falcon_later = Table(
        "falcon_later",
        metadata_public,
        Column("link", String(), nullable=False),
        Column("feed_message", String(), nullable=False),
        Column("created_ts_epoch", BigInteger(), nullable=False),
        Column("content_item_id", BigInteger(), nullable=False),
        Column("hold_expiration_ts_epoch", BigInteger(), nullable=False),
        Column("falcon_labels", String(), nullable=False),
        Column("type", String(), nullable=False),
        Column("recycle_ts_epoch", BigInteger(), nullable=False),
        Column(
            "social_account_id",
            UUID(as_uuid=True),
            ForeignKey("social_accounts.id"),
            nullable=False,
        ),
        UniqueConstraint("content_item_id", name="_uq_later_content_item_id_"),
        schema=brand_name,
    )
    metadata.create_all(bind=engine, tables=[falcon_later], checkfirst=True)

    falcon_never = Table(
        "falcon_never",
        metadata_public,
        Column("link", String(), nullable=False),
        Column("feed_message", String(), nullable=False),
        Column("created_ts_epoch", BigInteger(), nullable=False),
        Column("content_item_id", BigInteger(), nullable=False),
        Column("hold_expiration_ts_epoch", BigInteger(), nullable=False),
        Column(
            "falcon_labels", String(), nullable=False
        ),  # gives an issue with pyarrow library to keep labels column
        Column("type", String(), nullable=False),
        Column(
            "social_account_id",
            UUID(as_uuid=True),
            ForeignKey("social_accounts.id"),
            nullable=False,
        ),
        UniqueConstraint("content_item_id", name="_uq_never_content_item_id_"),
        schema=brand_name,
    )
    metadata.create_all(bind=engine, tables=[falcon_never], checkfirst=True)

    model_outcomes = Table(
        f"model_outcomes_{social_account}",
        metadata,
        Column("hours_from_anchor_point", Integer()),
        Column("cid", String()),
        Column("total_hourly_events_pageviews", Integer()),
        Column("anon_visits", Integer()),
        Column("direct_visits", Integer()),
        Column("referral_visits", Integer()),
        Column("internal_visits", Integer()),
        Column("search_visits", Integer()),
        Column("search_yahoo_visits", Integer()),
        Column("search_bing_visits", Integer()),
        Column("search_google_visits", Integer()),
        Column("social_visits", Integer()),
        Column("social_facebook_visits", Integer()),
        Column("social_instagram_visits", Integer()),
        Column("social_twitter_visits", Integer()),
        Column("final_social_posted", Integer()),
        Column("total_pageviews_prev_3hrs_2hrs", Integer()),
        Column("total_pageviews_prev_2hrs_1hrs", Integer()),
        Column("total_pageviews_prev_1hrs", Integer()),
        Column("total_pageviews_next_6hrs", Integer()),
        Column("anon_visits_prev_3hrs_2hrs", Integer()),
        Column("anon_visits_prev_2hrs_1hrs", Integer()),
        Column("anon_visits_prev_1hrs", Integer()),
        Column("anon_visits_next_6hrs", Integer()),
        Column("social_visits_prev_3hrs_2hrs", Integer()),
        Column("social_visits_prev_2hrs_1hrs", Integer()),
        Column("social_visits_prev_1hrs", Integer()),
        Column("social_visits_next_6hrs", Integer()),
        Column("social_facebook_visits_prev_3hrs_2hrs", Integer()),
        Column("social_facebook_visits_prev_2hrs_1hrs", Integer()),
        Column("social_facebook_visits_prev_1hrs", Integer()),
        Column("social_facebook_visits_next_6hrs", Integer()),
        Column("total_pageviews", Integer()),
        Column("total_anon_visits_prev_1hrs_3hrs", Integer()),
        Column("average_hourly_events", Float()),
        Column("brand", String()),
        Column("content_type", String()),
        Column("revision", Integer()),
        Column("headline", String()),
        Column("dek", String()),
        Column("contentsource", String()),
        Column("channel", String()),
        Column("subchannel", String()),
        Column("seo_title", String()),
        Column("seo_description", String()),
        Column("socialtitle", String()),
        Column("socialdescription", String()),
        Column("pub_date_epoch", BigInteger()),
        Column("original_pubdate_epoch", BigInteger()),
        Column("socialcopy", String()),
        Column("social_created_epoch_time", BigInteger()),
        Column("pubdate_diff_anchor_point", BigInteger()),
        Column("content_recency_when_accessed", BigInteger()),
        Column("content_type_description", String()),
        Column("is_hold", Boolean()),
        Column("is_scheduled", Boolean()),
        Column("is_optimized", Boolean()),
        Column("is_falcon_never", Boolean()),
        Column("is_falcon_later", Boolean()),
        Column("prediction_prob", Float()),
        schema=brand_name,
    )
    metadata.create_all(bind=engine, tables=[model_outcomes], checkfirst=True)

    socialflow_queue_posted_data = Table(
        f"socialflow_queue_posted_data_{social_account}",
        metadata,
        Column("hours_from_anchor_point", Integer()),
        Column("cid", String()),
        Column("total_hourly_events_pageviews", Integer()),
        Column("anon_visits", Integer()),
        Column("direct_visits", Integer()),
        Column("referral_visits", Integer()),
        Column("internal_visits", Integer()),
        Column("search_visits", Integer()),
        Column("search_yahoo_visits", Integer()),
        Column("search_bing_visits", Integer()),
        Column("search_google_visits", Integer()),
        Column("social_visits", Integer()),
        Column("social_facebook_visits", Integer()),
        Column("social_instagram_visits", Integer()),
        Column("social_twitter_visits", Integer()),
        Column("final_social_posted", Integer()),
        Column("total_pageviews_prev_3hrs_2hrs", Integer()),
        Column("total_pageviews_prev_2hrs_1hrs", Integer()),
        Column("total_pageviews_prev_1hrs", Integer()),
        Column("total_pageviews_next_6hrs", Integer()),
        Column("anon_visits_prev_3hrs_2hrs", Integer()),
        Column("anon_visits_prev_2hrs_1hrs", Integer()),
        Column("anon_visits_prev_1hrs", Integer()),
        Column("anon_visits_next_6hrs", Integer()),
        Column("social_visits_prev_3hrs_2hrs", Integer()),
        Column("social_visits_prev_2hrs_1hrs", Integer()),
        Column("social_visits_prev_1hrs", Integer()),
        Column("social_visits_next_6hrs", Integer()),
        Column("social_facebook_visits_prev_3hrs_2hrs", Integer()),
        Column("social_facebook_visits_prev_2hrs_1hrs", Integer()),
        Column("social_facebook_visits_prev_1hrs", Integer()),
        Column("social_facebook_visits_next_6hrs", Integer()),
        Column("total_pageviews", Integer()),
        Column("total_anon_visits_prev_1hrs_3hrs", Integer()),
        Column("average_hourly_events", Float()),
        Column("brand", String()),
        Column("content_type", String()),
        Column("revision", Integer()),
        Column("headline", String()),
        Column("dek", String()),
        Column("contentsource", String()),
        Column("channel", String()),
        Column("subchannel", String()),
        Column("seo_title", String()),
        Column("seo_description", String()),
        Column("socialtitle", String()),
        Column("socialdescription", String()),
        Column("pub_date_epoch", BigInteger()),
        Column("original_pubdate_epoch", BigInteger()),
        Column("socialcopy", String()),
        Column("social_created_epoch_time", BigInteger()),
        Column("pubdate_diff_anchor_point", BigInteger()),
        Column("content_recency_when_accessed", BigInteger()),
        Column("content_type_description", String()),
        Column("is_hold", Boolean()),
        Column("is_scheduled", Boolean()),
        Column("is_optimized", Boolean()),
        Column("is_falcon_never", Boolean()),
        Column("is_falcon_later", Boolean()),
        Column("prediction_prob", Float()),
        schema=brand_name,
    )
    metadata.create_all(
        bind=engine, tables=[socialflow_queue_posted_data], checkfirst=True
    )


def create_recs_dbs(metadata, metadata_public, engine):

    virality_outcomes = Table(
        "virality_outcomes",
        metadata_public,
        Column("cid", String(), nullable=False),
        Column("organization_id", String(), nullable=False),
        Column("content_type_description", String(), nullable=False),
        Column("content_url", String(), nullable=False),
        Column("prediction_prob", Float()),
        Column("content_recency_when_accessed", Integer(), nullable=False),
        Column(
            "social_account_id",
            UUID(as_uuid=True),
            ForeignKey("social_accounts.id"),
            nullable=False,
        ),
        UniqueConstraint("cid", name="_uq_outcomes_cid_"),
        schema="recommendations",  ##mention the schema here once it is updated
    )

    metadata.create_all(bind=engine, tables=[virality_outcomes], checkfirst=True)


@click.command()
@click.option("--is_brand", is_flag=True)
@click.option("--brand_name", help="Brand to Falcon'ize")
@click.option(
    "--social_account",
    help="Social account to create tables only model outcomes and socialflow outcomes tables",
)
@click.option("--mode", type=click.Choice(["dev", "prod"]))
def main(is_brand, brand_name, social_account, mode):
    """
    Creates the content data using sql queries from Presto
    """
    logger = logging.getLogger(__name__)
    logger.info(
        "Creating Brand Databases at "
        + pendulum.from_timestamp(int(CURR_UNIXTIME)).to_datetime_string()
    )

    vt_rds_data = VaultAccess(mode=mode).get_settings(settings_type="rds")
    engine = SQLAlchemyUtils.get_engine(vt_rds_data)
    metadata_public = MetaData(schema="public")

    if is_brand:
        # This section is for creating the databases for all brands
        metadata = MetaData(schema=brand_name)

        try:
            engine.execute(CreateSchema(brand_name))
        except Exception as e:
            logger.exception(
                f"Issue with the creation of the schema, might exist already {e}"
            )
            # sys.exit(0)

        logger.info("Finally creating the schema and databases !!!")
        create_common_dbs(metadata_public, engine)
        create_brand_dbs(
            metadata, metadata_public, engine, social_account, brand_name, mode
        )
    else:
        # This section is for creating the databases for recommendations
        logger.info("Comes to create the schema for recommendations !!")

        metadata = MetaData(schema="recommendations")

        try:
            engine.execute(CreateSchema("recommendations"))
        except Exception as e:
            logger.exception(
                f"Issue with the creation of the recommendations schema, might exist already {e}"
            )
            # sys.exit(0)

        logger.info("Finally creating the schema and databases !!!")
        create_common_dbs(
            metadata_public, engine
        )  # required, until table is mirrored, cannot use it
        create_recs_dbs(metadata, metadata_public, engine)


if __name__ == "__main__":
    # setup_logging()

    # not used in this stub but often useful for finding various files
    project_dir = Path(__file__).resolve().parents[2]
    # print(project_dir)

    # find .env automagically by walking up directories until it's found, then
    # load up the .env entries as environment variables
    load_dotenv(find_dotenv())

    main()
