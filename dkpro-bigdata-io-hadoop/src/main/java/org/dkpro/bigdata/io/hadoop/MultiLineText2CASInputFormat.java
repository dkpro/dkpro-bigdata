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
package org.dkpro.bigdata.io.hadoop;

import java.io.IOException;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.mapred.Reporter;
import org.apache.uima.cas.CAS;

import de.tudarmstadt.ukp.dkpro.core.api.metadata.type.DocumentMetaData;

/**
 * Input format for generating CAS instances from lines of text
 * 
 * All lines that lie in the boundaries of the input split are added
 * to the document text of one CAS
 * 
 * @author Johannes Simon
 * 
 */
public class MultiLineText2CASInputFormat extends FileInputFormat<Text, CASWritable> {

	/**
	 * Provide a custom implementation of this interface if you want to
	 * extract annotations from the &lt;Text, Text&gt; key/value lines in the input files.<br>
	 * <br>
	 * By default, i.e. if you do not set a custom AnnotationExtractor, no annotations
	 * are added to the initial CAS.
	 */
	public interface AnnotationExtractor {
		void extractAnnotations(Text key, Text value, CAS cas);
	}

	/**
	 * Tells Text2CASInputFormat to use a custom implementation of
	 * AnnotationExtractor. <br>
	 * Specifying an AnnotationExtractor implementation is optional
	 * and not necessary for this input format to work.
	 * 
	 * @param conf
	 *            Configuration object
	 * @param extractorClass
	 *            Implementation of DocumentTextExtractor
	 */
	public static void setAnnotationExtractorClass(Configuration conf,
			Class<? extends AnnotationExtractor> extractorClass) {
		conf.set("dkpro.uima.text2casinputformat.annotationextractor",
				extractorClass.getName());
	}

	/**
	 * Provide a custom implementation of this interface if you want the
	 * generated CAS instances to contain text different from the value of
	 * &lt;Text, Text&gt; key/value lines in the input files.<br>
	 * <br>
	 * This is useful e.g. if you want to remove HTML markup or if you want to
	 * extract text from the <b>key</b>, not from the <b>value</b>.<br>
	 * <br>
	 * By default, i.e. if you do not set a custom DocumentTextExtractor, the
	 * input line's value is used as the CAS document text.
	 */
	public interface DocumentTextExtractor {
		Text extractDocumentText(Text key, Text value);
	}

	/**
	 * Tells Text2CASInputFormat to use a custom implementation of
	 * DocumentTextExtractor. <br>
	 * By default, i.e. if you do not set a custom DocumentTextExtractor, the
	 * input line's value is used as the CAS document text.
	 * 
	 * @param conf
	 *            Configuration object
	 * @param extractorClass
	 *            Implementation of DocumentTextExtractor
	 */
	public static void setDocumentTextExtractorClass(Configuration conf,
			Class<? extends DocumentTextExtractor> extractorClass) {
		conf.set("dkpro.uima.text2casinputformat.documenttextextractor",
				extractorClass.getName());
	}

	/**
	 * Provide a custom implementation of this interface if you want the
	 * generated CAS instances to have metadata different from the value of
	 * &lt;Text, Text&gt; key/value lines in the input files.<br>
	 * <br>
	 * This is useful e.g. if you want to set the language or the URI of the
	 * document <br>
	 * By default, i.e. if you do not set a custom DocumentMetadataExtractor,
	 * the documentId and documentTitle are generated from the key/value pair
	 */
	public interface DocumentMetadataExtractor {
		void extractDocumentMetaData(Text key, Text value,
				DocumentMetaData metadata);
	}

	/**
	 * Tells Text2CASInputFormat to use a custom implementation of
	 * DocumentMetadataExtractor. <br>
	 * By default, i.e. if you do not set a custom DocumentMetadataExtractor,
	 * the documentId and documentTitle are generated from the key/value pair
	 * 
	 * @param conf
	 *            Configuration object
	 * @param extractorClass
	 *            Implementation of DocumentMetadataExtractor
	 */
	public static void setDocumentMetadataExtractorClass(Configuration conf,
			Class<? extends DocumentMetadataExtractor> extractorClass) {
		conf.set("dkpro.uima.multilinetext2casinputformat.documentmetadataextractor",
				extractorClass.getName());
	}

	/**
	 * Reads in CAS from JSON, XML or plain text.
	 * 
	 * <p>This is an internal class. It is only visible to hadoop.io package.</p>
	 * 
	 * @author Johannes Simon
	 * 
	 */
	private class MultiLineText2CASRecordReader extends
	GenericMultiLineRecordReader<CASWritable> {
		private final DocumentTextExtractor textExtractor;
		private final AnnotationExtractor annotationExtractor;
		private final DocumentMetadataExtractor metadataExtractor;

		public MultiLineText2CASRecordReader(FileSplit fileSplit, JobConf jobConf,
				Reporter reporter, DocumentTextExtractor textExtractor,
				AnnotationExtractor annotationExtractor,
				DocumentMetadataExtractor metadataExtractor) throws IOException {
			super(fileSplit, jobConf, reporter);
			this.textExtractor = textExtractor;
			this.annotationExtractor = annotationExtractor;
			this.metadataExtractor = metadataExtractor;
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
		public void convertValue(Text keyFrom, Text valueFrom,
				CASWritable valueTo) {
			CAS cas = valueTo.getCAS();
			cas.reset();
			Text doc = valueFrom;
			if (textExtractor != null) {
                doc = textExtractor.extractDocumentText(keyFrom, valueFrom);
            }

			cas.setDocumentText(doc.toString());

			if (annotationExtractor != null) {
                annotationExtractor.extractAnnotations(keyFrom, valueFrom, cas);
            }

			try {
				// add some simple metadata
				String key_as_str = keyFrom.toString();
				String key_abbrev = StringUtils.abbreviate(key_as_str, 50);
				DocumentMetaData metadata = DocumentMetaData.create(cas);
				metadata.setDocumentTitle(key_abbrev);
				metadata.setDocumentId(String.format("<%d>%s",
						key_as_str.hashCode(), key_abbrev));
				if (metadataExtractor != null) {
                    metadataExtractor.extractDocumentMetaData(keyFrom,
							valueFrom, metadata);
                }
			} catch (Exception e) {
				System.err.println("DocumentMetaData already present.");
			}

		}
	}

	@Override
	public RecordReader<Text, CASWritable> getRecordReader(InputSplit split,
			JobConf jobConf, Reporter reporter) throws IOException {
		DocumentTextExtractor textConverter = null;
		String textConverterClass = jobConf
				.get("dkpro.uima.text2casinputformat.documenttextextractor");
		if (textConverterClass != null) {
			try {
				textConverter = (DocumentTextExtractor) Class.forName(
						textConverterClass).newInstance();
			} catch (InstantiationException e) {
				e.printStackTrace();
			} catch (IllegalAccessException e) {
				e.printStackTrace();
			} catch (ClassNotFoundException e) {
				e.printStackTrace();
			}
		}

		AnnotationExtractor annotationExtractor = null;
		String annotationExtractorClass = jobConf
				.get("dkpro.uima.text2casinputformat.annotationextractor");
		if (annotationExtractorClass != null) {
			try {
				annotationExtractor = (AnnotationExtractor) Class.forName(
						annotationExtractorClass).newInstance();
			} catch (InstantiationException e) {
				e.printStackTrace();
			} catch (IllegalAccessException e) {
				e.printStackTrace();
			} catch (ClassNotFoundException e) {
				e.printStackTrace();
			}
		}
		
		DocumentMetadataExtractor metadataConverter = null;
		String metadataConverterClass = jobConf
				.get("dkpro.uima.text2casinputformat.documentmetadataextractor");
		if (metadataConverterClass != null) {
			try {
				metadataConverter = (DocumentMetadataExtractor) Class.forName(
						metadataConverterClass).newInstance();
			} catch (InstantiationException e) {
				e.printStackTrace();
			} catch (IllegalAccessException e) {
				e.printStackTrace();
			} catch (ClassNotFoundException e) {
				e.printStackTrace();
			}
		}
		return new MultiLineText2CASRecordReader((FileSplit) split, jobConf, reporter,
				textConverter, annotationExtractor, metadataConverter);
	}

}
