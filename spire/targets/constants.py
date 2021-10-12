from spire.config import config

#########
# SETUP #
#########
ENV = config.DEPLOYMENT_ENV
TIMEZONE = "EST"

#################
# VENDOR INPUTS #
#################

# Input URIs for load function
DFP_NETWORK_CLICKS_URI = "s3a://cn-data-vendor/dfp/NetworkClicks/orc/"
DFP_NETWORK_IMPRESSIONS_URI = "s3a://cn-data-vendor/dfp/NetworkImpressions/orc/"
NCS_CUSTOM_URI = "s3a://cn-data-reporting/spire/ncs/custom-parquet-v3/"
NCS_SYNDICATED_URI = "dbfs:/ncs/ncs_segments_2020/"
CDS_ALT_INDIV_URI = "s3a://cn-data-vendor/cds/inbound/active/alt_individual"
CDS_ALT_INDIV_DEMO_URI = "s3a://cn-data-vendor/cds/inbound/active/alt_individual_demo"
CDS_ALT_URI = "s3a://cn-data-vendor/cds/inbound/active/"
DAR_URI = "s3a://cn-data-reporting/spire/nielsen/dar/"
# GA_STREAM_URI Used for covid_pt and affiliate_pt
GA_STREAM_URI = "s3a://cn-data-vendor/google/analytics360/analytics_stream_delta/"
CUSTOM_URI = f"s3a://{config.SPIRE_DATA}/custom/vendor=custom/"

##########
# COMMON #
##########

# Filestore URIs
DATA_VENDOR_BUCKET = "cn-data-vendor"
DATA_VENDOR_URI = f"s3a://{DATA_VENDOR_BUCKET}/"
GA_NORM_BUCKET = "google/normalised_ga"
GA_NORM_TABLE_URI = f"s3a://{DATA_VENDOR_BUCKET}/{GA_NORM_BUCKET}"

# Regex
XID_REGEX = r"\b[0-9a-f]{8}\b-[0-9a-f]{4}-[0-9a-f]" r"{4}-[0-9a-f]{4}-\b[0-9a-f]{12}\b"
MCID_REGEX = r"\b[0-9]{38}"

# Offline ids
CDS_GA_MAP_IDS = ["mdw_id", "amg_user_id", "cds_account_number"]

DEFAULT_TARGET_VALUE = 1

TARGETS_OUTPUT_URI = f"s3a://{config.SPIRE_ENVIRON}/targets/"

###################
# VENDOR SPECIFIC #
###################

# Adobe
AAM_DESTINATION_ID = 77904
AAM_MCID_SEGS_DIR = "adobe/aamsegsmcid/"
SPIRE_SEGMENTS_S3_KEY = "data/adobe_segments/active_segments.csv"
SPIRE_AAM_SEGMENTS_URI = f"s3a://{config.SPIRE_ENVIRON}/{SPIRE_SEGMENTS_S3_KEY}"
ACTIVE_SEGMENTS_TABLE_URI = (
    f"s3a://{config.SPIRE_ENVIRON}/data/adobe_segments/active_segments"
)
MCID_SEGMENTS_TABLE_URI = (
    f"s3a://{config.SPIRE_ENVIRON}/data/adobe_segments/mcid_segments"
)

# Condenast
# GA infinity_id length fiter (for covid_pt and affiliate_pt)
GA_INFINITY_ID_LENGTH = 36
