pythonEnv=$0
script=$1
flagfile=$2

for i in {5..10}
do
  echo "Training with $i executors"
  SECONDS=0

  spark-submit --packages com.datastax.spark:spark-cassandra-connector_2.11:2.4.2 --deploy-mode client \
  --num-executors $i --conf spark.pyspark.python=$pythonEnv $script --flagfile=$flagfile

  duration=$SECONDS
  echo "Elapsed time: $(($duration / 60))min $(($duration % 60))"
done