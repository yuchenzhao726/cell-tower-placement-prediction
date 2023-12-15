#!/bin/bash

# Specify the HDFS directory containing input files
HDFS_DIR="/user/yl6211_nyu_edu/bdad_project/open_address"
TRAIN_DIR="/user/yl6211_nyu_edu/bdad_project/open_address_train_dataset"
TEST_DIR="/user/yl6211_nyu_edu/bdad_project/open_address_test_dataset"

# List all files in the HDFS directory
FILES=($(hadoop fs -ls $HDFS_DIR | grep "^-" | awk '{print $NF}'))

# Function to submit Spark job for a file
submit_spark_job() {
  local FILE=$1
  spark-submit --deploy-mode cluster --py-files pyproj-3.6.1.tar.gz,shapely-2.0.2.tar.gz building_footprint.py $FILE $TRAIN_DIR $TEST_DIR
}

# Batch size for parallel submissions
BATCH_SIZE=8
# Counter for parallel submissions
COUNT=0

# Loop through files and submit Spark jobs in parallel
for FILE in "${FILES[@]}"; do
  echo $FILE
  submit_spark_job "$FILE" &
  ((COUNT++))
  sleep 10

  # Check if the batch size is reached
  if [ $COUNT -eq $BATCH_SIZE ]; then
    # Wait for the current batch to complete
    wait
    # Reset counter
    COUNT=0
  fi
  
done

wait

exit

