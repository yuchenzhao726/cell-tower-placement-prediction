# nohup ./join_data_parallel.sh > run.log 2>&1 &
#!/bin/bash

POINTS_DATASET="bdad_proj/points_with_score_repar"
POP_DATASET="bdad_proj/pop_cnt_1km_repar"
PARTIAL_JOIN_OUTPUT="bdad_proj/partial_points_1d"

# List all files in the HDFS directory
FILES=($(hadoop fs -ls $POP_DATASET | grep "^-" | awk '{print $NF}'))

submit_spark_join_job() {
  local FILE=$1
  spark-submit join_pop_1d.py $POINTS_DATASET $FILE $PARTIAL_JOIN_OUTPUT
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
