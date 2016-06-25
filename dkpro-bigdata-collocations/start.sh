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
TARGET_DIR="colloc"
HEADNODE=headnode-02
mvn clean
mvn package
rm -rf target/lib/*jar
mvn dependency:copy-dependencies
scp run.sh $HEADNODE:$TARGET_DIR/
ssh $HEADNODE "chmod +x $TARGET_DIR/run.sh" 
rsync -avc target/de.tudarmstadt.ukp.dkpro.bigdata.collocations-0.1.0-SNAPSHOT.jar  $HEADNODE:$TARGET_DIR/
rsync -avc --delete target/lib/* --exclude "hadoop-*.jar" $HEADNODE:$TARGET_DIR/lib/
RUN="./run.sh de.tudarmstadt.ukp.dkpro.bigdata.collocations.CollocDriver -u --input $1 --output  $2 -wm SENTENCE -ws 3 -minV 3 --metric llr:0  -ow --maxRed 64"
nohup ssh $HEADNODE "cd $TARGET_DIR;pwd;$RUN" &
#RUN="./run.sh de.tudarmstadt.ukp.dkpro.hpc.collocations.CollocDriver -u --input wiki_english_tagged --output wikipedia_contingency_moresque_full --metric llr:0  -minV 0.1 -ow --maxRed 64"
#nohup ssh $HEADNODE "cd $TARGET_DIR;$RUN" &
