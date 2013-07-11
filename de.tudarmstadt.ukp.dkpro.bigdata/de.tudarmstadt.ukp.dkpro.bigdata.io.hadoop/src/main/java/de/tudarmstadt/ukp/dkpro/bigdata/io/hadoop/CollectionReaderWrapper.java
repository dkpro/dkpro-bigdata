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

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.uima.UIMAException;
import org.apache.uima.cas.CAS;
import org.apache.uima.collection.CollectionException;
import org.apache.uima.collection.CollectionReader;
import org.apache.uima.resource.ResourceInitializationException;
import org.apache.uima.resource.metadata.TypeSystemDescription;
import org.apache.uima.util.Progress;
import org.uimafit.factory.JCasFactory;
import org.uimafit.factory.TypeSystemDescriptionFactory;

/**
 * Wraps an existing CollectionReader (UIMA) instance as RecordReader<Text, CASWritable> (Hadoop).
 * 
 * @author Johannes Simon
 *
 */
public class CollectionReaderWrapper implements
		RecordReader<Text, CASWritable> {
	
	final private CollectionReader reader;
	
	private TypeSystemDescription typeSystem;
	
	public CollectionReaderWrapper(CollectionReader reader) {
		this.reader = reader;
		try {
			typeSystem = TypeSystemDescriptionFactory.createTypeSystemDescription();
		} catch (ResourceInitializationException e) {
			e.printStackTrace();
		}
	}

	@Override
	public void close() throws IOException {
		reader.close();
	}

	@Override
	public Text createKey() {
		return new Text();
	}

	@Override
	public CASWritable createValue() {
		return new CASWritable();
	}

	@Override
	public long getPos() throws IOException {
		Progress[] progressArr = reader.getProgress();
		for (Progress p : progressArr) {
			if (p.getUnit().equals(Progress.BYTES))
				return p.getCompleted();
		}
		
		return 0;
	}
	
	private static float getPercent(long part, long total) {
		if (total == 0)
			return 0.0f;
		return (float)part / (float)total;
	}

	@Override
	public float getProgress() throws IOException {
		Progress[] progressArr = reader.getProgress();
		for (Progress p : progressArr) {
			// Use the first progress type that reports a total > 0
			if (p.getTotal() > 0)
				return getPercent(p.getCompleted(), p.getTotal());
		}
		
		return 0.0f;
	}

	@Override
	public boolean next(Text key, CASWritable value) throws IOException {
		
		try {
			if (!reader.hasNext())
				return false;
			CAS nextCAS = JCasFactory.createJCas(typeSystem).getCas();
			reader.getNext(nextCAS);
			value.setCAS(nextCAS);
			
			return true;
		} catch (CollectionException e) {
			e.printStackTrace();
		} catch (UIMAException e) {
			e.printStackTrace();
		}
		
		// Error occured. Stop reading.
		return false;
	}

}
