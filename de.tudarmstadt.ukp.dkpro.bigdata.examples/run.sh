#!/bin/sh
#*******************************************************************************
# * Copyright 2012
# * Ubiquitous Knowledge Processing (UKP) Lab
# * Technische Universität Darmstadt
# * 
# * Licensed under the Apache License, Version 2.0 (the "License");
# * you may not use this file except in compliance with the License.
# * You may obtain a copy of the License at
# * 
# *   http://www.apache.org/licenses/LICENSE-2.0
# * 
# * Unless required by applicable law or agreed to in writing, software
# * distributed under the License is distributed on an "AS IS" BASIS,
# * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# * See the License for the specific language governing permissions and
# * limitations under the License.
# #******************************************************************************/
export CP=
for I in lib/*;do HADOOP_CLASSPATH=$HADOOP_CLASSPATH:$I;done
for I in lib/*;do CP=$CP,$I;done
export HADOOP_CLASSPATH=$HADOOP_CLASSPATH:$JAR
QUEUE=$1
JAR=$2
CLASS=$3
MEMORY=$4
shift;shift;shift;shift
PARAMS=$*
RUN="hadoop jar $JAR $CLASS -Dmapreduce.job.queuename=$QUEUE -Dmapred.job.map.memory.mb=$MEMORY -Dmapred.job.reduce.memory.mb=$MEMORY -libjars $JAR,$CP   $*"
echo $RUN
$RUN
