#!/bin/bash
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
. cluster.config
RUN="./run.sh $QUEUE $JAR $CLASS $MEMORY $1 $2"
TARGET_DIR=$CLASS-run
mvn clean
mvn package
mvn dependency:copy-dependencies
ssh $HEADNODE mkdir -p $TARGET_DIR
scp run.sh $HEADNODE:$TARGET_DIR/run.sh
ssh $HEADNODE chmod +x $TARGET_DIR/run.sh 
rsync -avc target/*.jar  $HEADNODE:$TARGET_DIR/
rsync -avc --delete target/lib/* --exclude "hadoop-*.jar" $HEADNODE:$TARGET_DIR/lib/
nohup ssh $HEADNODE "cd $TARGET_DIR;$RUN" &
