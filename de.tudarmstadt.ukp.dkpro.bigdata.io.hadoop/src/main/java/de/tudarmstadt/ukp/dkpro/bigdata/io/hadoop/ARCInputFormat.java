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

import java.io.BufferedInputStream;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.util.HashSet;
import java.util.Set;

import org.apache.commons.io.IOUtils;
import org.apache.commons.io.input.CountingInputStream;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.mapred.Reporter;
import org.jwat.arc.ArcHeader;
import org.jwat.arc.ArcReader;
import org.jwat.arc.ArcReaderFactory;
import org.jwat.arc.ArcRecordBase;
import org.jwat.common.HeaderLine;
import org.jwat.common.HttpHeader;
import org.jwat.common.PayloadWithHeaderAbstract;

//import de.uni_leipzig.asv.encodingdetector.utils.EncodingDetector;

/**
 * Creates ARCRecordReader for Crawler archives in ARC format
 * 
 * @author Johannes Simon
 */
public class ARCInputFormat
    extends FileInputFormat<Text, CrawlerRecord>
{
    @Override
    public RecordReader<Text, CrawlerRecord> getRecordReader(InputSplit inputSplit,
            JobConf jobConf, Reporter reporter)
        throws IOException
    {
        return new ARCRecordReader((FileSplit) inputSplit, jobConf);
    }

    /**
     * Reads Crawler archives in ARC format
     * 
     * @author Johannes Simon
     */
    public static class ARCRecordReader
        implements RecordReader<Text, CrawlerRecord>
    {
        private final long start;
        private final long end;
        private final CountingInputStream fsin;
        ArcReader arcReader = null;
        long lastRecordEnd = -1;
        private final Set<String> contentTypeWhitelist = new HashSet<String>();
        Configuration conf;

        /*
         * ======================== RecordReader Logic ============================
         */

        /**
         * Parses and sets configuration parameters according to a JobConf/Configuration instance
         */
        void configure(Configuration conf)
        {
            String contentTypeWhitelistStr = conf.get(
                    "dkpro.input.content-type-whitelist", "text/html");
            if (contentTypeWhitelistStr != null) {
                String[] contentTypes = contentTypeWhitelistStr.replace(" ", "").split(",");
                for (String ct : contentTypes) {
                    contentTypeWhitelist.add(ct);
                }
            }
            this.conf = conf;
        }

        /**
         * Creates an ARCRecordReader for the specified <code>split</code> of the input stream that
         * will start at the first valid ARC record header after <code>split.getStart()</code> and
         * continue until a record is read that goes past
         * <code>split.getStart() + split.getLength()</code>.
         */
        public ARCRecordReader(FileSplit split, JobConf jobConf)
            throws IOException
        {
            start = split.getStart();
            end = start + split.getLength();
            System.out.println("========== " + start + " " + end);

            configure(jobConf);

            // Open the file and seek to the start of the split
            Path file = split.getPath();
            FileSystem fs = file.getFileSystem(jobConf);
            fsin = new CountingInputStream(new BufferedInputStream(fs.open(split.getPath())));

            arcReader = ArcReaderFactory.getReader(fsin);
            // Start with the first valid record after offset "start"
            skipToNextRecord(start);
        }

        private void fillCrawlerRecord(ArcRecordBase record, CrawlerRecord crawlerRecord)
            throws IOException
        {
            // Fill CrawlerRecord
            ArcHeader header = record.header;
            // h.warcSegmentOriginIdUrl
            crawlerRecord.setURL(header.urlStr);
            // Usually ARC records contain the original webpage, including markup
            crawlerRecord.setIsHTML(true);
            crawlerRecord.setDate(header.archiveDate);
            // Usually not present in ARC records
            crawlerRecord.setOriginalLanguage(null);

            // It is important to *not* try to decode anything before we know the encoding.
            // As soon as we put any of this record content into a string, it is explicitly
            // converted
            // from/to some encoding. So before that, we have to guess the correct encoding,
            // otherwise
            // we'll potentially trash our data before we process it.
            byte[] buffer = IOUtils.toByteArray(record.getPayloadContent());
            Class<?> encodingDetectorClass = conf.getClass("dkpro.input.encodingdetector", DummyEncodingDetector.class);
			try {
				EncodingDetector encodingDetector = (EncodingDetector)encodingDetectorClass.newInstance();
	            // This is the encoding we'll use to decode the bytes into text
	            String encoding = encodingDetector.getBestEncoding(buffer);
	            crawlerRecord.setOriginalEncoding(encoding);
	            crawlerRecord.setContent(new String(buffer, encoding));
			} catch (InstantiationException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (IllegalAccessException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
        }

        @Override
        public boolean next(Text key, CrawlerRecord value)
            throws IOException
        {
            ArcRecordBase arcRecord = null;
            boolean atEnd = false;
            long bufferMarkAtEnd = 0;

            while ((arcRecord = arcReader.getNextRecord()) != null) {
                // Check if arcReader has definitely read over end mark when considering that reader
                // is buffered
                // (meaning that (fsin.getCount() > end) is true before we've read the last record
                // before end)
                lastRecordEnd = fsin.getCount();
                if (!atEnd && lastRecordEnd >= end) {
                    atEnd = true;
                    bufferMarkAtEnd = lastRecordEnd;
                }
                else if (atEnd && lastRecordEnd > bufferMarkAtEnd) {
                    break;
                }
                try {
                    // Make sure only text content is read
                    PayloadWithHeaderAbstract payloadHeader = arcRecord.getPayload()
                            .getPayloadHeaderWrapped();
                    if (payloadHeader == null || !(payloadHeader instanceof HttpHeader)) {
                        continue;
                    }
                    HeaderLine contentTypeHeader = payloadHeader.getHeader("Content-Type");
                    boolean skipContentType = true;
                    if (contentTypeHeader != null) {
                        for (String contentType : contentTypeWhitelist) {
                            if (contentTypeHeader.value.startsWith(contentType)) {
                                skipContentType = false;
                                break;
                            }
                        }
                    }

                    if (skipContentType) {
                        continue;
                    }

                    fillCrawlerRecord(arcRecord, value);
                    key.set(value.getURL());
                    return true;
                }
                catch (UnsupportedEncodingException e) {
                    // Skip unreadable records
                    System.err.println("WARNING: Skipping ARC record (byte offset "
                            + fsin.getCount()
                            + ") due to unsupported encoding. The record may contain binary data.");
                }
                catch (Exception e) {
                    // Skip any other records that produce exceptions
                    System.err
                            .println("WARNING: Skipping ARC record due to exception that occured while reading record:");
                    System.err.println(e.getMessage());
                    e.printStackTrace();
                }
            }

            return false;
        }

        @Override
        public Text createKey()
        {
            return new Text();
        }

        @Override
        public CrawlerRecord createValue()
        {
            return new CrawlerRecord();
        }

        @Override
        public long getPos()
            throws IOException
        {
            return fsin.getCount();
        }

        @Override
        public void close()
            throws IOException
        {
            fsin.close();
        }

        @Override
        public float getProgress()
            throws IOException
        {
            return ((float) (fsin.getCount() - start)) / ((float) (end - start));
        }

        /*
         * ======================== ARC Logic ============================
         */

        /**
         * Skips InputStream to next record past <code>start</code> or not at all if start is
         * exactly the start of a record
         */
        private void skipToNextRecord(long start)
            throws IOException
        {
            // Skip record by record until (position in input stream) >= start
            while (fsin.getCount() < start && arcReader.getNextRecord() != null) {
            }
            ;

            lastRecordEnd = fsin.getCount();
        }
    }
}