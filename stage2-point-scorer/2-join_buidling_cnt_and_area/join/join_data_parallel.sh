#!/bin/bash

ORIGINAL_DATASET="bdad_project/testset"
BUILDING_DATASET="bdad_project/partitioned_test_dataset"
PARTIAL_JOIN_OUTPUT="bdad_project/partial_join_test_stage2"

# List all files in the HDFS directory
FILES=($(hadoop fs -ls $BUILDING_DATASET | grep "^-" | awk '{print $NF}'))

submit_spark_join_job() {
  local FILE=$1
  spark-submit --class JoinBuildingData target/scala-2.12/join-building-data_2.12-1.0.jar $ORIGINAL_DATASET $FILE $PARTIAL_JOIN_OUTPUT
}

# Batch size for parallel submissions
BATCH_SIZE=8
# Counter for parallel submissions
COUNT=0

# Loop through files and submit Spark jobs in parallel
for FILE in "${FILES[@]}"; do
  if [[ "$FILE" != *"_SUCCESS"* ]]; then
    echo $FILE
    submit_spark_join_job "$FILE" &
    ((COUNT++))
    sleep 10

    # Check if the batch size is reached
    if [ $COUNT -eq $BATCH_SIZE ]; then
      # Wait for the current batch to complete
      wait
      # Reset counter
      COUNT=0
    fi
  fi
done

wait

exit
