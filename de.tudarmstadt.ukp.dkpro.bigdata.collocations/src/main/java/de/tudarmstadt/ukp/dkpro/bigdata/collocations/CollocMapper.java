/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package de.tudarmstadt.ukp.dkpro.bigdata.collocations;

import static org.apache.uima.fit.util.JCasUtil.select;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.regex.Pattern;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.mahout.math.function.ObjectIntProcedure;
import org.apache.mahout.math.map.OpenObjectIntHashMap;

import org.apache.uima.cas.CASException;
import org.apache.uima.fit.util.JCasUtil;
import org.apache.uima.jcas.JCas;
import org.apache.uima.jcas.tcas.Annotation;
import org.apache.uima.resource.metadata.ResourceMetaData;
import org.apache.uima.resource.metadata.impl.ResourceMetaData_impl;
import org.apache.uima.util.XMLParser;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.w3c.dom.Element;

import de.tudarmstadt.ukp.dkpro.bigdata.io.hadoop.CASWritable;
import de.tudarmstadt.ukp.dkpro.core.api.lexmorph.type.pos.POS;
import de.tudarmstadt.ukp.dkpro.core.api.segmentation.type.Lemma;
import de.tudarmstadt.ukp.dkpro.core.api.segmentation.type.Sentence;
import de.tudarmstadt.ukp.dkpro.core.api.segmentation.type.Stem;
import de.tudarmstadt.ukp.dkpro.core.api.segmentation.type.Token;

/**
 * Pass 1 of the Collocation discovery job which generated ngrams and emits ngrams an their
 * component n-1grams. Input is a SequeceFile&lt;Text,StringTuple&gt;, where the key is a document id and
 * the value is the tokenized documents.
 */
public class CollocMapper
    extends Mapper<Text, CASWritable, GramKey, Gram>
{

    private static final byte[] EMPTY = new byte[0];

    public static final String MAX_SHINGLE_SIZE = "maxShingleSize";

    private static final int DEFAULT_MAX_SHINGLE_SIZE = 2;

    public enum Count
    {
        NGRAM_TOTAL, OVERFLOW, MULTIWORD, EMITTED_UNIGRAM, SENTENCES, LEMMA, DOCSIZE, EMPTYDOC, WINDOWS
    }

    public enum Window
    {
        DOCUMENT, SENTENCE, S_WINDOW, C_WINDOW, FIXED
    }

    private static final Logger log = LoggerFactory.getLogger(CollocMapper.class);

    private boolean emitUnigrams;

    private Collection<String> multiwords;

    private ResourceMetaData metadata;

    private GramKey gramKey;

    private int window = 3;

    private Window windowMode = Window.SENTENCE;

    private final int MAX_NGRAMS = 5000;
    Pattern pattern = Pattern.compile(".*[\"\'#§$%&:\\+!,-]+.*");
    Class<? extends Annotation> annotation = Lemma.class;

    /**
     * Used by FeatureCountHadoopDriver to map each CAS to a set of features, e.g. its n-grams or
     * cooccurrences.
     */
    public interface CountableFeaturePairExtractor
    {
        public void configure(JobConf job);

        public void extract(final Context context, final JCas jcas, int lemmaCount);
    }

    /**
     * Collocation finder: pass 1 map phase.
     * <p>
     * Receives a token stream which gets passed through a Lucene ShingleFilter. The ShingleFilter
     * delivers ngrams of the appropriate size which are then decomposed into head and tail subgrams
     * which are collected in the following manner
     * </p>
     * 
     * <pre>
     * k:head_key,           v:head_subgram
     * k:head_key,ngram_key, v:ngram
     * k:tail_key,           v:tail_subgram
     * k:tail_key,ngram_key, v:ngram
     * </pre>
     * <p>
     * The 'head' or 'tail' prefix is used to specify whether the subgram in question is the head or
     * tail of the ngram. In this implementation the head of the ngram is a (n-1)gram, and the tail
     * is a (1)gram.
     * </p>
     * For example, given 'click and clack' and an ngram length of 3:
     * 
     * <pre>
     * k: head_'click and'                         v:head_'click and'
     * k: head_'click and',ngram_'click and clack' v:ngram_'click and clack'
     * k: tail_'clack',                            v:tail_'clack'
     * k: tail_'clack',ngram_'click and clack'     v:ngram_'click and clack'
     * </pre>
     * <p>
     * Also counts the total number of ngrams encountered and adds it to the counter
     * CollocDriver.Count.NGRAM_TOTAL
     * </p>
     * 
     * @throws IOException
     *             if there's a problem with the ShingleFilter reading data or the collector
     *             collecting output.
     */
    @Override
    protected void map(Text key, CASWritable value, final Context context)
        throws IOException, InterruptedException
    {

        // ShingleFilter sf = new ShingleFilter(new
        // IteratorTokenStream(value.getEntries().iterator()), maxShingleSize);
        gramKey = new GramKey();
        try {
            // int count = 0; // ngram count

            // final CAS aCAS = CasCreationUtils.createCas(asList(this.metadata));
            // final String xml = value.toString();
            //
            // XCASDeserializer.deserialize(new StringInputStream(xml), aCAS);

            final JCas jcas = value.getCAS().getJCas();

            int lemmaCount = jcas.getAnnotationIndex(Lemma.type).size();
            context.getCounter(Count.LEMMA).increment(lemmaCount);

            context.getCounter(Count.DOCSIZE).increment(jcas.getDocumentText().length());

            if (this.windowMode == Window.DOCUMENT) {
                extractWholeDocument(context, jcas, lemmaCount);
            }
            else if (this.windowMode == Window.SENTENCE) {
                extractSentence(context, jcas, lemmaCount);
            }
            else if (this.windowMode == Window.C_WINDOW)
             {
                extractWindow(context, jcas, lemmaCount);
            // OpenObjectIntHashMap<String> ngrams = new OpenObjectIntHashMap<String>(lemmaCount *
            // 4);
            // OpenObjectIntHashMap<String> unigrams = new OpenObjectIntHashMap<String>(lemmaCount);
            // int sentenceCount=0;
            // for (final Annotation sentence : select(jcas, Sentence.class)) {
            //
            // sentenceCount++;
            // count += collectCooccurencesFromCoveringAnnotation(context, jcas, sentence, ngrams,
            // unigrams);
            // if (count > 10000) {
            // flushCollocations(context, ngrams, unigrams);
            // // I suspect the clear method is not working properly
            // ngrams = new OpenObjectIntHashMap<String>(lemmaCount * 4);
            // unigrams = new OpenObjectIntHashMap<String>(lemmaCount);
            // context.getCounter(Count.SENTENCES).increment(sentenceCount);
            // context.getCounter(Count.NGRAM_TOTAL).increment(count);
            // count = 0;
            // sentenceCount = 0;
            // }
            // }
            // flushCollocations(context, ngrams, unigrams);
            // context.getCounter(Count.NGRAM_TOTAL).increment(count);
            }

        }
        catch (NullPointerException e1) {
            context.getCounter(Count.EMPTYDOC).increment(1);
        }
        catch (CASException e1) {
            // TODO Auto-generated catch block
            e1.printStackTrace();
        }
        finally {
            // Closeables.closeQuietly(sf);
        }
    }

    private void extractWindow(org.apache.hadoop.mapreduce.Mapper.Context context, JCas jcas,
            int lemmaCount)
    {
        OpenObjectIntHashMap<String> ngrams = new OpenObjectIntHashMap<String>(lemmaCount * 4);
        OpenObjectIntHashMap<String> unigrams = new OpenObjectIntHashMap<String>(lemmaCount);
        int counta = 0;
        int ngramcount = 0;
        // int count = collectCooccurencesFromCas(context, jcas, ngrams, unigrams);
        ArrayList<Lemma> terms = new ArrayList<Lemma>();

        for (final Lemma term : JCasUtil.select(jcas, Lemma.class)) {
            terms.add(term);
        }
        for (int wcount = 0; wcount < (terms.size() / window); wcount++) {
            for (int i = 0; i < window; i++) {
                if ((wcount * window) + i > terms.size()) {
                    break;
                }
                String termText = terms.get((wcount * window) + i).getValue().toLowerCase();

                if (!isValid(termText)) {

                    continue;
                }
                int countb = 0;
                context.getCounter(Count.WINDOWS).increment(1);
                unigrams.adjustOrPutValue(termText, 1, 1);
                for (int j = 0; j < window; j++) {
                    if ((wcount * window) + j > terms.size()) {
                        break;
                    }
                    String termText2 = terms.get((wcount * window) + j).getValue().toLowerCase();
                    // // out.set(termText, termText2);
                    // ngrams.adjustOrPutValue(termText+" "+termText2, 1, 1);
                    // count++;
                    if (!isValid(termText2)) {
                        continue;
                    }

                    ngrams.adjustOrPutValue(termText + "\t" + termText2, 1, 1);

                    if (ngramcount++ > 10000) {
                        flushCollocations(context, ngrams, unigrams);
                        context.getCounter(Count.NGRAM_TOTAL).increment(i);
                        ngrams = new OpenObjectIntHashMap<String>(lemmaCount * 4);
                        unigrams = new OpenObjectIntHashMap<String>(lemmaCount);
                        ngramcount = 0;

                    }
                    context.getCounter("test", "iteration").increment(1);
                    if (countb++ > 1000) {
                        break;
                    }
                }
                if (counta++ > 1000) {
                    break;
                }

            }
        }

        flushCollocations(context, ngrams, unigrams);

        context.getCounter(Count.NGRAM_TOTAL).increment(ngramcount);

    }

    private int extractSentence(final Context context, final JCas jcas, int lemmaCount)
    {
        OpenObjectIntHashMap<String> ngrams = new OpenObjectIntHashMap<String>(lemmaCount * 4);
        OpenObjectIntHashMap<String> unigrams = new OpenObjectIntHashMap<String>(lemmaCount);
        int sentenceCount = 0;
        int count = 0;
        Annotation[] previous = new Annotation[window];
        for (final Annotation sentence : select(jcas, Sentence.class)) {
            for (int j = 0; j < previous.length - 1; j++) {
                previous[j] = previous[j + 1];
            }
            previous[previous.length - 1] = sentence;
            sentenceCount++;
            count += collectCooccurencesFromCoveringAnnotation(context, jcas, sentence, ngrams,
                    unigrams);
            if (count > 10000) {
                flushCollocations(context, ngrams, unigrams);
                // I suspect the clear method is not working properly
                ngrams = new OpenObjectIntHashMap<String>(lemmaCount * 4);
                unigrams = new OpenObjectIntHashMap<String>(lemmaCount);
                context.getCounter(Count.SENTENCES).increment(sentenceCount);
                context.getCounter(Count.NGRAM_TOTAL).increment(count);
                count = 0;
                sentenceCount = 0;
            }
        }
        flushCollocations(context, ngrams, unigrams);
        return count;
    }

    private void extractWholeDocument(final Context context, final JCas jcas, int lemmaCount)
    {
        OpenObjectIntHashMap<String> ngrams = new OpenObjectIntHashMap<String>(lemmaCount * 4);
        OpenObjectIntHashMap<String> unigrams = new OpenObjectIntHashMap<String>(lemmaCount);
        int counta = 0;

        int i = 0;
        int j = 0;
        // int count = collectCooccurencesFromCas(context, jcas, ngrams, unigrams);
        for (final Lemma term : JCasUtil.select(jcas, Lemma.class)) {
            String termText = term.getValue().toLowerCase();

            POS pos = null;
            for (POS p : JCasUtil.selectCovered(jcas, POS.class, term)) {
                pos = p;
            }

            if (!isValid(termText)) {

                continue;
            }
            int countb = 0;
            unigrams.adjustOrPutValue(termText, 1, 1);
            for (final Lemma term2 : JCasUtil.select(jcas, Lemma.class)) {
                final String termText2 = term2.getValue().toLowerCase();
                // // out.set(termText, termText2);
                // ngrams.adjustOrPutValue(termText+" "+termText2, 1, 1);
                // count++;
                if (!isValid(termText2)) {
                    continue;
                }

                ngrams.adjustOrPutValue(termText + "\t" + termText2, 1, 1);

                if (i++ > 10000) {
                    flushCollocations(context, ngrams, unigrams);
                    context.getCounter(Count.NGRAM_TOTAL).increment(i);
                    ngrams = new OpenObjectIntHashMap<String>(lemmaCount * 4);
                    unigrams = new OpenObjectIntHashMap<String>(lemmaCount);
                    i = 0;

                }
                context.getCounter("test", "iteration").increment(1);
                if (countb++ > 1000) {
                    break;
                }
            }
            if (counta++ > 1000) {
                break;
            }

        }

        flushCollocations(context, ngrams, unigrams);

        context.getCounter(Count.NGRAM_TOTAL).increment(i);

    }

    private String getValue(final Annotation term)
    {

        if (term instanceof Token) {
            return ((Token) term).getCoveredText().toLowerCase();
        }
        if (term instanceof Lemma) {
            return ((Lemma) term).getValue().toLowerCase();
        }
        if (term instanceof Stem) {
            return ((Stem) term).getValue().toLowerCase();
        }

        throw new UnsupportedOperationException("Unknown annotation type "
                + term.getClass().getCanonicalName());
    }

    private void flushCollocations(final Context context, OpenObjectIntHashMap<String> ngrams,
            OpenObjectIntHashMap<String> unigrams)
    {

        ngrams.forEachPair(new ObjectIntProcedure<String>()
        {
            @Override
            public boolean apply(String term, int frequency)
            {
                // obtain components, the leading (n-1)gram and the trailing unigram.
                int i = term.lastIndexOf('\t');
                if (i != -1) { // bigram, trigram etc

                    try {
                        Gram ngram = new Gram(term, frequency, Gram.Type.NGRAM);
                        Gram head = new Gram(term.substring(0, i), frequency, Gram.Type.HEAD);
                        Gram tail = new Gram(term.substring(i + 1), frequency, Gram.Type.TAIL);

                        gramKey.set(head, EMPTY);
                        context.write(gramKey, head);

                        gramKey.set(head, ngram.getBytes());
                        context.write(gramKey, ngram);

                        gramKey.set(tail, EMPTY);
                        context.write(gramKey, tail);

                        gramKey.set(tail, ngram.getBytes());
                        context.write(gramKey, ngram);

                    }
                    catch (IOException e) {
                        throw new IllegalStateException(e);
                    }
                    catch (InterruptedException e) {
                        throw new IllegalStateException(e);
                    }
                }
                return true;
            }
        });

        unigrams.forEachPair(new ObjectIntProcedure<String>()
        {
            @Override
            public boolean apply(String term, int frequency)
            {
                try {
                    Gram unigram = new Gram(term, frequency, Gram.Type.UNIGRAM);
                    gramKey.set(unigram, EMPTY);

                    context.write(gramKey, unigram);
                }
                catch (IOException e) {
                    throw new IllegalStateException(e);
                }
                catch (InterruptedException e) {
                    throw new IllegalStateException(e);
                }
                return true;
            }
        });
        unigrams.clear();
        ngrams.clear();

    }

    private int collectCooccurencesFromCoveringAnnotation(final Context context, JCas jcas,
            final Annotation sentence, OpenObjectIntHashMap<String> ngrams,
            OpenObjectIntHashMap<String> unigrams)
    {
        int count = 0;
        int i = 0;
        if (sentence != null) {
            for (final Lemma term : JCasUtil.selectCovered(jcas, Lemma.class, sentence)) {
                final String termText = term.getValue().toLowerCase();

                if (!isValid(termText)) {

                    continue;
                }

                String left = termText;
                unigrams.adjustOrPutValue(left, 1, 1);
                for (final Lemma term2 : JCasUtil.selectCovered(jcas, Lemma.class, sentence)) {
                    final String termText2 = term2.getValue().toLowerCase();
                    // // out.set(termText, termText2);
                    // ngrams.adjustOrPutValue(termText+" "+termText2, 1, 1);
                    // count++;
                    if (!isValid(termText2)) {
                        continue;
                    }
                    if (!left.equals(termText2)) {
                        ngrams.adjustOrPutValue(left + "\t" + termText2, 1, 1);
                    }
                    count++;

                }
                if (i++ > 1000) {
                    context.getCounter(Count.OVERFLOW).increment(1);
                    return count;
                }

            }
        }

        return count;
    }

    private boolean isValid(final String termText)
    {
        return !(termText.length() == 1 || pattern.matcher(termText).matches() || termText
                .contains(".."));
    }

    @Override
    protected void setup(Context context)
        throws IOException, InterruptedException
    {
        super.setup(context);
        Configuration conf = context.getConfiguration();
        this.window = conf.getInt(CollocDriver.WINDOW_SIZE, 3);
        this.windowMode = Window
                .valueOf(conf.get(CollocDriver.WINDOW_TYPE, Window.SENTENCE.name()));
        this.emitUnigrams = conf.getBoolean(CollocDriver.EMIT_UNIGRAMS,
                CollocDriver.DEFAULT_EMIT_UNIGRAMS);
        this.metadata = new ResourceMetaData_impl();
        final Element aElement;
        final XMLParser aParser = org.apache.uima.UIMAFramework.getXMLParser();
        // try {
        //
        // this.metadata = aParser.parseResourceMetaData(new XMLInputSource(new StringInputStream(
        // Metadata.getMetadata()), new File(".")));
        // }
        // catch (final InvalidXMLException e1) {
        // // TODO Auto-generated catch block
        // e1.printStackTrace();
        // }

        if (log.isInfoEnabled()) {
            // log.info("Max Ngram size is {}", this.maxShingleSize);
            log.info("Emit Unitgrams is {}", emitUnigrams);
            log.info("Window Mode is {}", this.windowMode.name());
            log.info("Window Size is {}", window);
            log.info("Emit Unitgrams is {}", emitUnigrams);

        }
    }
}
