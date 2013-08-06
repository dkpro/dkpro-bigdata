#!/bin/sh
TARGET_DIR="colloc"
HEADNODE=headnode-02
mvn clean
mvn package
mvn dependency:copy-dependencies
scp run.sh $HEADNODE:$TARGET_DIR/run.sh
ssh $HEADNODE chmod +x $TARGET_DIR/run.sh 
rsync -avc target/de.tudarmstadt.ukp.dkpro.bigdata.collocations-0.1.0-SNAPSHOT.jar  $HEADNODE:$TARGET_DIR/
rsync -avc --delete target/lib/* --exclude "hadoop-*.jar" $HEADNODE:$TARGET_DIR/lib/
RUN="./run.sh de.tudarmstadt.ukp.dkpro.bigdata.collocations.CollocDriver -u --input test --output wkt_sentence_all_measures -wm SENTENCE --metric llr:0  -minV 3 -ow --maxRed 64"
nohup ssh $HEADNODE "cd $TARGET_DIR;$RUN" &
#RUN="./run.sh de.tudarmstadt.ukp.dkpro.hpc.collocations.CollocDriver -u --input wiki_english_tagged --output wikipedia_contingency_moresque_full --metric llr:0  -minV 0.1 -ow --maxRed 64"
#nohup ssh $HEADNODE "cd $TARGET_DIR;$RUN" &
