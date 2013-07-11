#!/bin/sh
export JAR=de.tudarmstadt.ukp.dkpro.bigdata.collocations-0.1.0-SNAPSHOT.jar
export HADOOP_CLASSPATH=
export CP=
for I in lib/*;do HADOOP_CLASSPATH=$HADOOP_CLASSPATH:$I;done
for I in lib/*;do CP=$CP,$I;done
export HADOOP_CLASSPATH=$HADOOP_CLASSPATH:$JAR
echo $HADOOP_CLASSPATH
RUN="hadoop jar $JAR $1 -libjars $JAR,$CP  $2 $3 $4 $5 $6 $7 $8 $9 $10 $11 $12 $13"
echo $RUN
$RUN
