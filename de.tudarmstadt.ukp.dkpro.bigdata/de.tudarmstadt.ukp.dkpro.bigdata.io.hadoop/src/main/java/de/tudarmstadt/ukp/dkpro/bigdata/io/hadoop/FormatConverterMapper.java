package de.tudarmstadt.ukp.dkpro.bigdata.io.hadoop;

import static org.apache.uima.fit.factory.TypeSystemDescriptionFactory.createTypeSystemDescription;

import java.io.ByteArrayInputStream;
import java.io.IOException;

import org.apache.commons.io.IOUtils;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;
import org.apache.tools.ant.filters.StringInputStream;
import org.apache.uima.cas.CAS;
import org.apache.uima.cas.impl.XCASDeserializer;
import org.apache.uima.resource.ResourceInitializationException;
import org.apache.uima.util.CasCreationUtils;
import org.xml.sax.SAXException;

public class FormatConverterMapper
    implements Mapper<Text, Text, Text, CASWritable>
{

    @Override
    public void configure(JobConf job)
    {
        // TODO Auto-generated method stub

    }

    @Override
    public void close()
        throws IOException
    {
        // TODO Auto-generated method stub

    }

    @Override
    public void map(Text key, Text value, OutputCollector<Text, CASWritable> output,
            Reporter reporter)
        throws IOException
    {
        try {
            CAS cas = CasCreationUtils.createCas(createTypeSystemDescription(), null, null);

            XCASDeserializer.deserialize(new StringInputStream(value.toString()), cas);

            // XCASDeserializer.deserialize(IOUtils.toInputStream(value.toString(), "utf8"), cas);
            CASWritable casWritable = new BinCasWritable();
            casWritable.setCAS(cas);
            output.collect(key, casWritable);
            reporter.incrCounter("hpz", "processed cas", 1);
            if (cas.getDocumentText() == null)
                reporter.incrCounter("hpz", "document text null", 1);
            else
                reporter.incrCounter("hpz", "doc size", cas.getDocumentText().length());

        }
        catch (Exception e) {

            reporter.incrCounter("hpz", "exception " + e.getMessage(), 1);
            e.printStackTrace(System.err);
        }

    }

}
