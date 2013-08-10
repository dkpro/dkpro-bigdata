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
