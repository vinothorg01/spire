#!/bin/bash
set -e
export PATH="/home/akatiyal/.conda/envs/datasci-virality/bin/:$PATH"
cd /home/akatiyal/datasci-virality
make BRAND_NAME=vogue MODE=dev SOCIAL_ACCOUNT_TYPE=facebook_page log_outputs_kafka
make BRAND_NAME=vogue MODE=dev SOCIAL_ACCOUNT_TYPE=twitter log_outputs_kafka
