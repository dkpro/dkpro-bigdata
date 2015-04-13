# Introduction #



# Example #
```
public class UimaPipelineOnHadoop
    extends DkproHadoopDriver
{
   
    public CollectionReader buildCollectionReader()
        throws ResourceInitializationException
    {
        return createReader(TextReader.class, TextReader.PARAM_SOURCE_LOCATION, "src/test/resources/text",
                TextReader.PARAM_PATTERNS, new String[] { INCLUDE_PREFIX + "*.txt" },
                TextReader.PARAM_LANGUAGE, "en");

    }
    public AnalysisEngineDescription buildMapperEngine(Configuration job)
        throws ResourceInitializationException
    {
        AnalysisEngineDescription tokenizer = createEngineDescription(BreakIteratorSegmenter.class);
        AnalysisEngineDescription stemmer = createEngineDescription(SnowballStemmer.class,
                SnowballStemmer.PARAM_LANGUAGE, "en"); 
        return createEngineDescription(tokenizer, stemmer);

    }  
    public static void main(String[] args) throws Exception
    {
            UimaPipelineOnHadoop pipeline = new UimaPipelineOnHadoop();
            pipeline.setMapperClass(DkproMapper.class);
            pipeline.setReducerClass(DkproReducer.class);
            ToolRunner.run(new Configuration(), pipeline, args);
    }
	@Override
	public void configure(JobConf job) {
		job.set("mapreduce.job.queuename", "smalljob");	
	}

}

```
## Instructions ##
  * Write a class that extends DKProHadoopDriver
  * Implement abstract methods:
    * buildCollectionReader
      * Executed within the driver.
      * Can access files on the computer where the job is started.
      * Copies data to the cluster and converts it to SequenceFile
    * buildMapperEngine
      * Executed on the compute node to construct your Analysis Engine
    * configure

Moreover, you will need a main method that will use hadoop to submit your job to the cluster. In general you can use the main method in above example as a template.