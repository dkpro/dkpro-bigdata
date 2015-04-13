## Setting up a DKPro Big Data Project in Eclipse ##

Projects that are using DKPro BigData to run on the cluster are just regular DKPro/Maven projects. Additionally you will need a set of scripts that will copy your project to the cluster and execute it there.
You will find those scripts by checking out dkpro-bigdata-examples or you can download them using the following links:
run.sh
start.sh
## Getting from Maven Central ##

DKPro BigData is now on maven central, It does not depend on any SNAPSHOT-Releases of dkpro anymore and you should be able to just include the following dependency to your pom.xml to get started;
```
   <dependency>
      <groupId>de.tudarmstadt.ukp.dkpro.bigdata</groupId>
        <artifactId>
           de.tudarmstadt.ukp.dkpro.bigdata.hadoop
        </artifactId>
        <version>0.1.0</version>
   </dependency> 
     
```