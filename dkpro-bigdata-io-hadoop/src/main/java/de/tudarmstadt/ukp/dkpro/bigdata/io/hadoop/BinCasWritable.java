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

import static org.apache.uima.cas.impl.Serialization.deserializeCAS;
import static org.apache.uima.cas.impl.Serialization.serializeWithCompression;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.DataOutputStream;
import java.io.IOException;

import org.apache.uima.cas.CASRuntimeException;
import org.apache.uima.resource.ResourceInitializationException;

/**
 * This Writable serializes the CAS in binary form *without* Typesystem. Use this only when you will
 * process the files with _exactly_ the same Typesystem again (e.g. for intermediate output.
 * 
 * @author zorn
 * 
 */
public class BinCasWritable
    extends CASWritable
{

    @Override
    public void readFields(DataInput in)
        throws IOException
    {
        int dataLength = in.readInt();
        byte[] data = new byte[dataLength];
        in.readFully(data);

        ByteArrayInputStream bis = new ByteArrayInputStream(data);
        try {

            deserializeCAS(cas, bis, cas.getTypeSystem(), null);
        }
        catch (CASRuntimeException e) {
            throw new IOException(e);
        }
        catch (ResourceInitializationException e) {
            throw new IOException(e);
        }

    }

    @Override
    public void write(DataOutput out)
        throws IOException
    {
        ByteArrayOutputStream aOS = new ByteArrayOutputStream(400);
        DataOutputStream docOS = new DataOutputStream(aOS);

        try {

            serializeWithCompression(cas, docOS, cas.getTypeSystem());
            docOS.flush();
            docOS.close();

            out.writeInt(aOS.size());
            byte[] byteArray = aOS.toByteArray();

            out.write(byteArray);
        }
        catch (CASRuntimeException e) {
            throw new IOException(e);
        }
        catch (ResourceInitializationException e) {
            throw new IOException(e);

        }

    }

}
