#!/bin/bash
set -e
export PATH="/home/akatiyal/.conda/envs/datasci-virality/bin/:$PATH"
cd /home/akatiyal/datasci-virality
make BRAND_NAME=bon-appetit MODE=prod SOCIAL_ACCOUNT_TYPE=facebook_page socialposted_data_content_kafka
make BRAND_NAME=bon-appetit MODE=prod SOCIAL_ACCOUNT_TYPE=twitter socialposted_data_content_kafka
