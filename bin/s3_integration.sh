#!/bin/bash

"""
Install hadoop dependencies for AWS and configure credentials provider
"""

curl -sSL https://search.maven.org/remotecontent?filepath=org/apache/hadoop/hadoop-aws/2.7.4/hadoop-aws-2.7.4.jar > ${SPARK_HOME}/jars/hadoop-aws-2.7.4.jar
curl -sSL https://search.maven.org/remotecontent?filepath=com/amazonaws/aws-java-sdk/1.7.4/aws-java-sdk-1.7.4jar > ${SPARK_HOME}/jars/aws-java-sdk-1.7.4.jar

mkdir $SPARK_HOME/conf
echo spark.hadoop.fs.s3a.aws.credentials.provider=com.amazonaws.auth.profile.ProfileCredentialsProvider,com.amazonaws.auth.profile.ProfileCredentialsProvider,org.apache.hadoop.fs.s3a.AnonymousAWSCredentialsProvider >> $SPARK_HOME/conf/spark-defaults.conf
