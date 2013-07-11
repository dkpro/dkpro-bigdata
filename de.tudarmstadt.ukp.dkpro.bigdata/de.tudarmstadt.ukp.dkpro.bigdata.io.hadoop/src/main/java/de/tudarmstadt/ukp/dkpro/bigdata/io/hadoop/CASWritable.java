/*******************************************************************************
 * Copyright 2013
 * TU Darmstadt, FG Sprachtechnologie
 * 
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 ******************************************************************************/
package de.tudarmstadt.ukp.dkpro.bigdata.io.hadoop;

import static org.uimafit.factory.TypeSystemDescriptionFactory.createTypeSystemDescription;

import java.io.ByteArrayInputStream;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.io.StringWriter;

import org.apache.hadoop.io.Writable;
import org.apache.uima.cas.CAS;
import org.apache.uima.cas.impl.XCASDeserializer;
import org.apache.uima.cas.impl.XCASSerializer;
import org.apache.uima.resource.ResourceInitializationException;
import org.apache.uima.util.CasCreationUtils;
import org.apache.uima.util.XMLSerializer;
import org.xml.sax.SAXException;

/**
 * hadoop.io.Writable implementation for use with a CAS.
 * 
 * @author Johannes Simon
 * 
 */
public class CASWritable
    implements Writable
{

    private CAS cas;

    public CASWritable()
    {
        try {
            cas = CasCreationUtils.createCas(createTypeSystemDescription(), null, null);
        }
        catch (ResourceInitializationException e) {
            e.printStackTrace();
        }
    }

    public void setCAS(CAS cas)
    {
        this.cas = cas;
    }

    public CAS getCAS()
    {
        return cas;
    }

    /**
     * Returns XML (XCAS) representation of this CAS, stripped of all newlines.
     */
    @Override
    public String toString()
    {
        if (cas == null)
            return "null";
        // Use StringWriter instead of OutputStream for XMLSerializer so that
        // we don't need to worry about string encoding (utf-8 vs. utf-16, etc.)
        StringWriter writer = new StringWriter();
        XMLSerializer xmlSerializer = new XMLSerializer(writer);
        XCASSerializer casSerializer = new XCASSerializer(cas.getTypeSystem());
        try {
            casSerializer.serialize(cas, xmlSerializer.getContentHandler());
        }
        catch (SAXException e) {
            e.printStackTrace();
        }
        catch (IOException e) {
            e.printStackTrace();
        }
        String xml = writer.toString();
        xml = xml.replace("\n", "");

        return xml;
    }

    @Override
    public void readFields(DataInput in)
        throws IOException
    {
        int dataLength = in.readInt();
        byte[] data = new byte[dataLength];
        in.readFully(data);
        String serializedCAS = new String(data, "UTF-8");
        try {
            XCASDeserializer.deserialize(new ByteArrayInputStream(serializedCAS.getBytes("UTF-8")),
                    cas);
        }
        catch (SAXException e) {
            e.printStackTrace();
        }
    }

    @Override
    public void write(DataOutput out)
        throws IOException
    {
        String str = toString();
        byte[] data = str.getBytes("UTF-8");
        out.writeInt(data.length);
        out.write(data);
    }

}
