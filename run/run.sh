export SPARK_JAVA_OPTS="-Xss20M "
input=/tmp/mf/test.data
autput=/tmp/mf/output
numIterations=100
rank=20
lambda=0.01
block=10
name=mf-zhengchen

hadoop fs -rmr /tmp/mf/output
SPARK_JAR=$SPARK_HOME/assembly/target/scala-2.10/spark-assembly_2.10-0.9.0-incubating-hadoop2.2.0.jar $SPARK_HOME/bin/spark-class org.apache.spark.deploy.yarn.Client \
  --jar /home/spark/zc/mf/target/scala-2.10/mf_2.10-1.0.jar \
  --class MF \
  --args yarn-standalone \
  --args $input \
  --args $output \
  --args $numIterations \
  --args $rank \
  --args $lambda \
  --args $block \
  --master-memory 4g \
  --worker-memory 4g \
  --worker-cores 1 \
  --name $name 
