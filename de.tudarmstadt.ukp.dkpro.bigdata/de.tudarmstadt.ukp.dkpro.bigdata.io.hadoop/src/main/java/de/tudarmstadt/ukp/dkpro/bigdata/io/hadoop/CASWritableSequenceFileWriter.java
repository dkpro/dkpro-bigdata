/*******************************************************************************
 * Copyright 2010,2012
 * Ubiquitous Knowledge Processing (UKP) Lab
 * Technische Universität Darmstadt
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 ******************************************************************************/
package de.tudarmstadt.ukp.dkpro.bigdata.io.hadoop;

import java.io.File;
import java.io.IOException;
import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.SequenceFile.Writer;
import org.apache.hadoop.io.Text;
import org.apache.uima.analysis_engine.AnalysisEngineProcessException;
import org.apache.uima.fit.component.JCasConsumer_ImplBase;
import org.apache.uima.fit.descriptor.ConfigurationParameter;
import org.apache.uima.jcas.JCas;
import org.apache.uima.resource.ResourceInitializationException;

import de.tudarmstadt.ukp.dkpro.core.api.metadata.type.DocumentMetaData;
import de.tudarmstadt.ukp.dkpro.core.api.parameter.ComponentParameters;

/**
 * Write XMI to HDFS sequence files
 * 
 * @author Richard Eckart de Castilho, Hans-Peter Zorn
 */
public class CASWritableSequenceFileWriter
    extends JCasConsumer_ImplBase
{
    /**
     * The folder to write the generated XMI files to.
     */
    public static final String PARAM_PATH = ComponentParameters.PARAM_TARGET_LOCATION;
    @ConfigurationParameter(name = PARAM_PATH, mandatory = true)
    private File path;

    /**
     * Location to write the type system to. If this is not set, a file called typesystem.xml will
     * be written to the XMI output path. If this is set, it is expected to be a file relative to
     * the current work directory or an absolute file. <br>
     * If this parameter is set, the {@link #PARAM_COMPRESS} parameter has no effect on the type
     * system. Instead, if the file name ends in ".gz", the file will be compressed, otherwise not.
     */
    public static final String PARAM_TYPE_SYSTEM_FILE = "TypeSystemFile";
    @ConfigurationParameter(name = PARAM_TYPE_SYSTEM_FILE, mandatory = false)
    private File typeSystemFile;

    /**
     * Enabled/disable hadoop compression. If this is set, all files will have the ".gz" ending.
     */
    public static final String PARAM_COMPRESS = "Compress";

    @ConfigurationParameter(name = PARAM_COMPRESS, mandatory = true, defaultValue = "false")
    private boolean compress;

    public static final String PARAM_FS = "HadoopFs";
    @ConfigurationParameter(name = PARAM_FS, mandatory = true, defaultValue = "hdfs://10.130.21.11:8020")
    private String fileSystemName;

    private Writer writer;
    private int i = 0;;

    @Override
    public void initialize(org.apache.uima.UimaContext context)
        throws org.apache.uima.resource.ResourceInitializationException
    {
        super.initialize(context);
        final Configuration conf = new Configuration(false);

        this.path = new File((String) context.getConfigParameterValue(PARAM_PATH));
        conf.set("fs.default.name", this.fileSystemName);
        // Compress Map output
        if (this.compress) {
            System.out.println("compressing");
            conf.set("mapred.output.compress", "true");

            conf.set("mapred.map.output.compression.codec",
                    "org.apache.hadoop.io.compress.SnappyCodec");
        }
        else
            conf.set("mapred.output.compress", "false");

        // conf.set("mapred.compress.map.output", "true");
        //
        // // Compress MapReduce output
        // conf.set("mapred.output.compression", "org.apache.hadoop.io.compress.SnappyCodec");
        // // FileOutputFormat.setOutputCompressorClass(conf, SnappyCodec.class);

        final String filename = this.path + "/" + "part-00000";
        try {
            final FileSystem fs = FileSystem.get(URI.create(filename), conf);
            final Path path = new Path(URI.create(filename).toString());
            this.writer = SequenceFile.createWriter(fs, conf, path, Text.class,
                    BinCasWritable.class);

        }
        catch (final IOException e) {
            throw new ResourceInitializationException(e);
        }

    };

    @Override
    public void collectionProcessComplete()
        throws AnalysisEngineProcessException
    {
        try {
            this.writer.close();
        }
        catch (final IOException e) {
            // TODO Auto-generated catch block
            throw new AnalysisEngineProcessException();
        }
    };

    @Override
    public void process(JCas aJCas)
        throws AnalysisEngineProcessException
    {
        final DocumentMetaData meta = DocumentMetaData.get(aJCas);

        String documentKey = createKeyFromDocument(meta);

        try {
            CASWritable casWritable = new BinCasWritable();
            casWritable.setCAS(aJCas.getCas());
            this.writer.append(new Text(documentKey), casWritable);

            // TypeSystemUtil.typeSystem2TypeSystemDescription(aJCas.getTypeSystem()).toXML(typeOS);
        }
        catch (final Exception e) {
            throw new AnalysisEngineProcessException(e);
        }

    }

    /**
     * Try to figure our a meaningful key from the document metadata
     * 
     * @param meta
     * @param baseUri
     * @param docUri
     * @return
     */
    private String createKeyFromDocument(final DocumentMetaData meta)
    {
        final String baseUri = meta.getDocumentBaseUri();
        final String docUri = meta.getDocumentUri();

        String relativeDocumentPath = "doc_" + this.i++;
        if (baseUri != null) {
            if ((docUri == null) || !docUri.startsWith(baseUri)) {
                throw new IllegalStateException("Base URI [" + baseUri
                        + "] is not a prefix of document URI [" + docUri + "]");
            }
            relativeDocumentPath = docUri.substring(baseUri.length());
        }
        else {
            if (meta.getDocumentId() == null) {

                relativeDocumentPath = meta.getDocumentTitle(); // TODO: Bad Hack!
            }
            else {
                relativeDocumentPath = meta.getDocumentId();
            }
        }
        return relativeDocumentPath;
    }
}
