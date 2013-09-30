package de.tudarmstadt.ukp.dkpro.bigdata.io.hadoop;

import static org.apache.uima.fit.factory.TypeSystemDescriptionFactory.createTypeSystemDescription;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.fail;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;

import org.apache.uima.cas.CAS;
import org.apache.uima.util.CasCreationUtils;
import org.junit.Ignore;
import org.junit.Test;

public class CASWritableTest
{

    private static final String testString = "Lorem ipsum dolor sit amet, consetetur sadipscing elitr, sed diam nonumy eirmod tempor invidunt ut labore et dolore magna aliquyam erat, sed diam voluptua.";
    protected Class<? extends CASWritable> writable = CASWritable.class;

    @Test
    public void testCASWritable()
        throws InstantiationException, IllegalAccessException
    {
        CASWritable casWritable = writable.newInstance();
        assertNotNull(casWritable.getCAS());

    }

    @Test
    public void testReadWriteFields()
    {
        try {
            CAS cas = CasCreationUtils.createCas(createTypeSystemDescription(), null, null);
            cas.setDocumentText(testString);

            CASWritable casWritable = writable.newInstance();
            casWritable.setCAS(cas);
            ByteArrayOutputStream os = new ByteArrayOutputStream();
            ObjectOutputStream oos = new ObjectOutputStream(os);
            casWritable.write(oos);
            oos.close();
            casWritable = writable.newInstance();
            ByteArrayInputStream bis = new ByteArrayInputStream(os.toByteArray());
            ObjectInputStream ois = new ObjectInputStream(bis);
            casWritable.readFields(ois);
            assertEquals(casWritable.getCAS().getDocumentText(), testString);

        }
        catch (Exception e) {
            e.printStackTrace();
            fail(e.getMessage());
        }

    }

}
