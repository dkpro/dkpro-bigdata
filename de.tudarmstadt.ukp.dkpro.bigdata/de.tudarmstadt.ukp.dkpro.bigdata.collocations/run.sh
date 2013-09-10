#!/bin/sh
export JAR=de.tudarmstadt.ukp.dkpro.bigdata.collocations-0.1.0-SNAPSHOT.jar
export HADOOP_CLASSPATH=
export CP=
for I in lib/*;do HADOOP_CLASSPATH=$HADOOP_CLASSPATH:$I;done
for I in lib/*;do CP=$CP,$I;done
export HADOOP_CLASSPATH=$HADOOP_CLASSPATH:$JAR
echo $HADOOP_CLASSPATH
export CLASS=$1
shift
RUN="hadoop jar $JAR $CLASS -Dmapreduce.job.queuename=ukp -libjars $JAR,$CP  $*"
echo $RUN
$RUN
