/*******************************************************************************
 * Copyright 2012,2013
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

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInput;
import java.io.DataInputStream;
import java.io.DataOutput;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.util.zip.DeflaterInputStream;
import java.util.zip.DeflaterOutputStream;
import java.util.zip.InflaterInputStream;

import org.apache.uima.cas.CAS;
import org.apache.uima.cas.CASException;
import org.apache.uima.cas.CASRuntimeException;
import org.apache.uima.cas.impl.CASMgrSerializer;
import org.apache.uima.cas.impl.TypeSystemImpl;
import org.apache.uima.resource.ResourceInitializationException;

import static org.apache.uima.cas.impl.Serialization.deserializeCAS;
import static org.apache.uima.cas.impl.Serialization.serializeCASMgr;
import static org.apache.uima.cas.impl.Serialization.serializeWithCompression;

public class BinCasWithTypeSystemWritable
    extends CASWritable
{
    TypeSystemImpl ts = null;

    @Override
    public void readFields(DataInput in)
        throws IOException
    {
        ByteArrayInputStream bis = null;
        DataInputStream datis = null;
        InflaterInputStream iis = null;
        ObjectInputStream ois = null;
        try {
            int dataLength = in.readInt();
            byte[] data = new byte[dataLength];
            in.readFully(data);
            bis = new ByteArrayInputStream(data);
            datis = new DataInputStream(bis);
            int offset = datis.readInt();
            datis.close();

            iis = new InflaterInputStream(bis);

            ois = new ObjectInputStream(iis);
            if (ts == null) {
                CASMgrSerializer casMgrSerializer = (CASMgrSerializer) ois.readObject();
                ts = casMgrSerializer.getTypeSystem();
                ts.commit();
            }
            bis.close();
            bis = new ByteArrayInputStream(data, offset + 4, dataLength - 4 - offset);
            deserializeCAS(cas, bis, ts, null);

        }
        catch (CASRuntimeException e) {
            throw new IOException(e);
        }
        catch (ResourceInitializationException e) {
            throw new IOException(e);
        }
        catch (ClassNotFoundException e) {
            throw new IOException(e);
        }
        finally {
            ois.close();
            iis.close();
            datis.close();
            bis.close();

        }

    }

    @Override
    public void write(DataOutput out)
        throws IOException
    {
        ByteArrayOutputStream aOS = new ByteArrayOutputStream(400);
        DataOutputStream docOS = new DataOutputStream(aOS);

        try {
            writeTypeSystem(cas, docOS);
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

    private void writeTypeSystem(CAS cas, DataOutputStream aOS)
        throws IOException
    {

        ByteArrayOutputStream bos = new ByteArrayOutputStream(512);
        DeflaterOutputStream dos = new DeflaterOutputStream(bos);
        ObjectOutputStream typeOS = new ObjectOutputStream(dos);

        CASMgrSerializer casMgrSerializer;
        try {
            casMgrSerializer = serializeCASMgr(cas.getJCas().getCasImpl());
            typeOS.writeObject(casMgrSerializer);
        }
        catch (CASException e) {
            throw new IOException(e);
        }
        typeOS.flush();
        dos.flush();
        dos.finish();
        aOS.writeInt(bos.size());
        bos.writeTo(aOS);
        aOS.flush();
    }
}
