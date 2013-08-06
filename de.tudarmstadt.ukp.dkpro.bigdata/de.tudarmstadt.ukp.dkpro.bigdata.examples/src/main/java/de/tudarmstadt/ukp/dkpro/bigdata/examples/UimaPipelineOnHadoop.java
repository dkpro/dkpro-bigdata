package de.tudarmstadt.ukp.dkpro.bigdata.examples;

import static org.uimafit.factory.AnalysisEngineFactory.createAggregateDescription;
import static org.uimafit.factory.AnalysisEngineFactory.createPrimitiveDescription;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.SequenceFileInputFormat;
import org.apache.hadoop.util.ToolRunner;
import org.apache.uima.analysis_engine.AnalysisEngineDescription;
import org.apache.uima.resource.ResourceInitializationException;

import de.tudarmstadt.ukp.dkpro.bigdata.hadoop.DkproHadoopDriver;
import de.tudarmstadt.ukp.dkpro.bigdata.hadoop.DkproMapper;
import de.tudarmstadt.ukp.dkpro.bigdata.hadoop.DkproReducer;
import de.tudarmstadt.ukp.dkpro.core.stanfordnlp.StanfordSegmenter;
import de.tudarmstadt.ukp.dkpro.core.treetagger.TreeTaggerPosLemmaTT4J;

public class UimaPipelineOnHadoop
    extends DkproHadoopDriver
{
    //
    // @Override
    // public CollectionReader buildCollectionReader()
    // throws ResourceInitializationException
    // {
    // return createCollectionReader(TextReader.class, TextReader.PARAM_PATH,
    // "/home/zorn/AMBIENT", TextReader.PARAM_PATTERNS, new String[] { "[+]*" });
    //
    // }

    @Override
    public AnalysisEngineDescription buildMapperEngine(Configuration job)
        throws ResourceInitializationException
    {
        AnalysisEngineDescription aggreggate = null;
        AnalysisEngineDescription indexTermAnnotator;
        try {
            AnalysisEngineDescription tokenizer = createPrimitiveDescription(
                    StanfordSegmenter.class, StanfordSegmenter.PARAM_CREATE_TOKENS, true);

            AnalysisEngineDescription treeTagger = createPrimitiveDescription(
                    TreeTaggerPosLemmaTT4J.class, TreeTaggerPosLemmaTT4J.PARAM_WRITE_LEMMA, "en",
                    TreeTaggerPosLemmaTT4J.PARAM_WRITE_POS, true);
            // AnalysisEngineDescription stemmer = createPrimitiveDescription(SnowballStemmer.class,
            // SnowballStemmer.PARAM_LANGUAGE, "en");

            // indexTermAnnotator = createPrimitiveDescription(IndexTermAnnotator.class,
            // IndexTermAnnotator.PARAM_CHANGE_TO_LOWER_CASE, true,
            // IndexTermAnnotator.PARAM_PATHS, new String[] { Stem.class.getName() + "/value",
            // Token.class.getName(), Lemma.class.getName() + "/value" });
            // AnalysisEngineDescription writer =
            // createPrimitiveDescription(LuceneIndexWriter.class,
            // IndexWriterBase.PARAM_INDEX_PATH, "$dir/index",
            // LuceneIndexWriter.PARAM_KEEP_DOCS_WITH_NO_INDEXTERM_ANNOTATIONS, false,
            // LuceneIndexWriter.PARAM_CREATE_NEW_INDEX, true,
            // LuceneIndexWriter.PARAM_USE_TERM_TYPE_SUFFIX, true);
            aggreggate = createAggregateDescription(tokenizer, treeTagger);

        }
        catch (ResourceInitializationException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
        return aggreggate;
    }

    @Override
    public AnalysisEngineDescription buildReducerEngine(Configuration job)
        throws ResourceInitializationException
    {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public Class getInputFormatClass()
    {
        return SequenceFileInputFormat.class;
    }

    @Override
    public void configure(JobConf job)
    {
        // TODO Auto-generated method stub

    }

    public static void main(String[] args)
    {

        try {
            UimaPipelineOnHadoop pipeline = new UimaPipelineOnHadoop();
            pipeline.setMapperClass(DkproMapper.class);
            pipeline.setReducerClass(DkproReducer.class);

            ToolRunner.run(new Configuration(), pipeline, args);
        }
        catch (Exception e) {
            e.printStackTrace();
        }
    }

}
