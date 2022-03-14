#!/bin/bash
set -e
export PATH="/home/akatiyal/.conda/envs/datasci-virality/bin/:$PATH"
cd /home/akatiyal/datasci-virality
make BRAND_NAME=vanity-fair MODE=dev SOCIAL_ACCOUNT_TYPE=facebook_page socialcopy_data_kafka
make BRAND_NAME=vanity-fair MODE=dev SOCIAL_ACCOUNT_TYPE=twitter socialcopy_data_kafka
