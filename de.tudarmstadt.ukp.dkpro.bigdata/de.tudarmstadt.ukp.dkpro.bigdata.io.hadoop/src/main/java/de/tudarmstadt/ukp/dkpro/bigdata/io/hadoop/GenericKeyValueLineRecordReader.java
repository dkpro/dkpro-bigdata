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
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.KeyValueLineRecordReader;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.mapred.Reporter;

/**
 * Generic version of KeyValueLineRecordReader from org.apache.hadoop.mapred package.<br/>
 * <br/>
 * Use this class if you need to read plain-text key/value pairs and emit potentially non-text
 * keys and values based on the input text key/value.
 * For this, <code>convertKey()</code> and <code>convertValue()</code> need to be implemented by your subclass.<br/>
 * <br/>
 * Useful for e.g. reading XML-serialized UIMA CASes. In this case, the key could be
 * the URL of the original web document, and the value the annotated document (i.e. CAS).
 * 
 * @author Johannes Simon
 */
public abstract class GenericKeyValueLineRecordReader<K, V> implements RecordReader<K, V> {

	private final KeyValueLineRecordReader lineReader;
	
	public GenericKeyValueLineRecordReader(FileSplit split, JobConf jobConf, Reporter reporter) throws IOException {
		lineReader = new KeyValueLineRecordReader(jobConf, split);
	}
	
	@Override
	public boolean next(K key, V value) throws IOException {
		Text textKey = lineReader.createKey();
		Text textValue = lineReader.createValue();
		boolean success = lineReader.next(textKey, textValue);
		if (success) {
			convertKey(textKey, textValue, key);
			convertValue(textKey, textValue, value);
		}
		
		return success;
	}

	// The following methods only delegate functionality to lineReader
	
	protected abstract void convertValue(Text textKey, Text textValue, V value);
	protected abstract void convertKey(Text textKey, Text textValue, K key);

	@Override public void close() throws IOException { lineReader.close(); }
	@Override public long getPos() throws IOException { return lineReader.getPos(); }
	@Override public float getProgress() throws IOException { return lineReader.getProgress(); }
	
}