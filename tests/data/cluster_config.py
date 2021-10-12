generic_config = {
    "run_name": "my spark task",
    "new_cluster": {
        "spark_version": "7.3.x-scala2.12",
        "node_type_id": "r3.xlarge",
        "aws_attributes": {"availability": "ON_DEMAND"},
        "num_workers": 10,
    },
    "libraries": [
        {"jar": "dbfs:/my-jar.jar"},
        {"maven": {"coordinates": "org.jsoup:jsoup:1.7.2"}},
    ],
    "spark_jar_task": {"main_class_name": "com.databricks.ComputeModels"},
}

# Note that the instance_profile_arn has been set to empty strings for the test
# but to actually use this config, that value must be populated
test_task_config = {
    "task_type": "target",
    "run_name": "spire-ncs-targets",
    "cluster_configuration": {
        "spark_version": "9.0.x-scala2.12",
        "node_type_id": "r5.2xlarge",
        "aws_attributes": {
            "zone_id": "us-east-1b",
            "first_on_demand": 1,
            "availability": "SPOT_WITH_FALLBACK",
            "instance_profile_arn": "",
            "spot_bid_price_percent": 100,
            "ebs_volume_type": "GENERAL_PURPOSE_SSD",
            "ebs_volume_count": 3,
            "ebs_volume_size": 100,
        },
        "autoscale": {"min_workers": 1, "max_workers": 8},
        "custom_tags": {"Group": "Data Science", "Project": "Spire"},
    },
}
