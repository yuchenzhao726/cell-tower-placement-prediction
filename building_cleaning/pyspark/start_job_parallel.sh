#!/bin/bash

# Specify the HDFS directory containing input files
HDFS_DIR="/user/yl6211_nyu_edu/bdad_project/dataset2"
TRAIN_DIR="/user/yl6211_nyu_edu/bdad_project/training_dataset"
TEST_DIR="/user/yl6211_nyu_edu/bdad_project/test_dataset"

# List all files in the HDFS directory
FILES=($(hadoop fs -ls $HDFS_DIR | grep "^-" | awk '{print $NF}'))

# Function to submit Spark job for a file
submit_spark_job() {
  local FILE=$1
  spark-submit --py-files pyproj-3.6.1.tar.gz,shapely-2.0.2.tar.gz building_footprint.py $FILE $TRAIN_DIR $TEST_DIR
}

# Loop through files and submit Spark jobs in parallel
for FILE in "${FILES[@]}"; do
  submit_spark_job "$FILE" &
  sleep 10
done

exit

