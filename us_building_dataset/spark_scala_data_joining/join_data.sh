#!/bin/bash

ORIGINAL_DATASET="bdad_project/training_set_3"
BUILDING_DATASET="bdad_project/open_address_train_dataset"
PARTIAL_JOIN_OUTPUT="bdad_project/partial_join_output"
INPUT_DIR="$ORIGINAL_DATASET"

# By default output the combined dataset to OUTPUT_DIR1
OUTPUT_DIR1="bdad_project/join_output1"
OUTPUT_DIR2="bdad_project/join_output2"
# 0=false 1=true
OUTPUT_IS_DIR1=1
COMBINED_OUTPUT="$OUTPUT_DIR1"

# List all files in the HDFS directory
FILES=($(hadoop fs -ls $BUILDING_DATASET | grep "^-" | awk '{print $NF}'))

submit_spark_join_job() {
  local FILE=$1
  spark-submit --class JoinBuildingData join/target/scala-2.12/join-building-data_2.12-1.0.jar $ORIGINAL_DATASET $FILE $PARTIAL_JOIN_OUTPUT
}

submit_spark_combine_job() {
  if [ "$OUTPUT_IS_DIR1" = 1 ]; then
    COMBINED_OUTPUT="$OUTPUT_DIR1"
  else
    COMBINED_OUTPUT="$OUTPUT_DIR2"
  fi
  echo "COMBINED_INPUT: $INPUT_DIR"
  echo "COMBINED_OUTPUT: $COMBINED_OUTPUT"
  spark-submit --class DataCombiner combine/target/scala-2.12/combine-building-data_2.12-1.0.jar $INPUT_DIR $PARTIAL_JOIN_OUTPUT $COMBINED_OUTPUT
}

for FILE in "${FILES[@]}"; do
  if [[ "$FILE" != *"_SUCCESS"* ]]; then
    echo $FILE
    submit_spark_join_job "$FILE" &
    wait

    submit_spark_combine_job
    wait
    INPUT_DIR="$COMBINED_OUTPUT"
    OUTPUT_IS_DIR1=$((OUTPUT_IS_DIR1 ^ 1))
  fi
done

exit
