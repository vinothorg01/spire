#!/bin/bash
set -e
export PATH="/home/akatiyal/.conda/envs/datasci-virality/bin/:$PATH"
cd /home/akatiyal/datasci-virality
make BRAND_NAME=conde-nast-traveler MODE=dev content_data