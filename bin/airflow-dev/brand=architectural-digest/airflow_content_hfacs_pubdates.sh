#!/bin/bash
set -e
export PATH="/home/akatiyal/.conda/envs/datasci-virality/bin/:$PATH"
cd /home/akatiyal/datasci-virality
make BRAND_NAME=architectural-digest MODE=dev content_hfacs_pubdates