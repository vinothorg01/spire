#!/bin/bash
set -e
export PATH="/home/akatiyal/.conda/envs/datasci-virality/bin/:$PATH"
cd /home/akatiyal/datasci-virality
make BRAND_NAME=architectural-digest MODE=dev SOCIAL_ACCOUNT_TYPE=facebook_page predict_kafka
make BRAND_NAME=architectural-digest MODE=dev SOCIAL_ACCOUNT_TYPE=twitter predict_kafka
