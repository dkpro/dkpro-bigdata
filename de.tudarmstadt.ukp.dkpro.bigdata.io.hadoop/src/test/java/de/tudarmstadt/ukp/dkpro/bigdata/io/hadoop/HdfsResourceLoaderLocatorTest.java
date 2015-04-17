package de.tudarmstadt.ukp.dkpro.bigdata.io.hadoop;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.hdfs.MiniDFSCluster.Builder;
import org.apache.uima.collection.CollectionReader;
import org.apache.uima.fit.factory.CollectionReaderFactory;
import org.apache.uima.fit.factory.ExternalResourceFactory;
import org.apache.uima.fit.factory.JCasFactory;
import org.apache.uima.jcas.JCas;
import org.apache.uima.resource.ExternalResourceDescription;
import org.junit.Before;
import org.junit.Test;

import de.tudarmstadt.ukp.dkpro.bigdata.io.hadoop.HdfsResourceLoaderLocator;
import de.tudarmstadt.ukp.dkpro.core.io.text.TextReader;

public class HdfsResourceLoaderLocatorTest
{
    static MiniDFSCluster cluster = null;
    static String defaultdir = null;

    static Configuration conf = new Configuration();

    @Test
    public void testDKProResourceLoading()
        throws Exception
    {
        ExternalResourceDescription hdfsResource = ExternalResourceFactory
                .createExternalResourceDescription(HdfsResourceLoaderLocator.class,
                        HdfsResourceLoaderLocator.PARAM_FILESYSTEM,
                        "hdfs://localhost:" + cluster.getNameNodePort());

        CollectionReader reader = CollectionReaderFactory.createReader(TextReader.class,
                TextReader.KEY_RESOURCE_RESOLVER, hdfsResource, TextReader.PARAM_SOURCE_LOCATION,
                "hdfs:/hdfsLocator", TextReader.PARAM_PATTERNS, "*.data");

        List<String> documents = readDocuments(reader);

        assertEquals(2, documents.size());
        assertTrue(documents.get(0).equals("Text of file one."));
        assertTrue(documents.get(1).equals("Text of file two."));

    }

    private List<String> readDocuments(CollectionReader aReader)
        throws Exception
    {
        List<String> documentContents = new ArrayList<String>();
        while (aReader.hasNext()) {
            JCas createJCas = JCasFactory.createJCas();
            aReader.getNext(createJCas.getCas());
            String text = createJCas.getDocumentText();
            documentContents.add(text);
        }
        return documentContents;
    }

    @Before
    public void copyFilesIntoHdfs()
        throws Exception
    {
        String folder = "hdfsLocator";

        Builder builder = new MiniDFSCluster.Builder(conf);
        cluster = builder.clusterId("testcluster").build();
        cluster.waitActive();

        DistributedFileSystem fs = cluster.getFileSystem();

        defaultdir = "hdfs://localhost:" + cluster.getNameNodePort() + "/";
        conf.set("fs.default.dir", defaultdir);
        fs.mkdirs(new Path("/" + folder));
        fs.copyFromLocalFile(new Path("src/test/resources/hdfsLocator/one.data"), new Path(
                defaultdir + "/" + folder));
        fs.copyFromLocalFile(new Path("src/test/resources/hdfsLocator/two.data"), new Path(
                defaultdir + "/" + folder));

        RemoteIterator<LocatedFileStatus> listFiles = fs.listFiles(new Path(defaultdir + "/"
                + folder), true);
        while (listFiles.hasNext()) {
            LocatedFileStatus next = listFiles.next();
            System.out.println(next.getPath().toString());
        }
    }

}
