#!/bin/bash

# Specify the HDFS directory containing input files
HDFS_DIR="/user/yl6211_nyu_edu/bdad_project/dataset2"
TRAIN_DIR="/user/yl6211_nyu_edu/bdad_project/training_set"
TEST_DIR="/user/yl6211_nyu_edu/bdad_project/test_set"

# List all files in the HDFS directory
FILES=($(hadoop fs -ls $HDFS_DIR | grep "^-" | awk '{print $NF}'))

# Function to submit Spark job for a file
submit_spark_job() {
  local FILE=$1
  spark-submit --packages org.locationtech.geotrellis:geotrellis-vector_2.12:2.3.1,org.locationtech.geotrellis:geotrellis-proj4_2.12:2.3.1 --class BuildingFootprint target/scala-2.12/building-footprint_2.12-1.0.jar $FILE $TRAIN_DIR $TEST_DIR
}

# Loop through files and submit Spark jobs in parallel
for FILE in "${FILES[@]}"; do
  submit_spark_job "$FILE" &
  sleep 10
done

exit

