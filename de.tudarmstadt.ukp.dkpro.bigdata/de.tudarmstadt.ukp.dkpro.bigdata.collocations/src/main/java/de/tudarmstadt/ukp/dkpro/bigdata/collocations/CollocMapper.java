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

import java.io.File;
import java.io.IOException;
import java.util.Collection;
import java.util.regex.Pattern;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.mahout.math.function.ObjectIntProcedure;
import org.apache.mahout.math.map.OpenObjectIntHashMap;
import org.apache.tools.ant.filters.StringInputStream;
import org.apache.uima.cas.CASException;
import org.apache.uima.fit.util.JCasUtil;
import org.apache.uima.jcas.JCas;
import org.apache.uima.jcas.tcas.Annotation;
import org.apache.uima.resource.metadata.ResourceMetaData;
import org.apache.uima.resource.metadata.impl.ResourceMetaData_impl;
import org.apache.uima.util.InvalidXMLException;
import org.apache.uima.util.XMLInputSource;
import org.apache.uima.util.XMLParser;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.w3c.dom.Element;

import de.tudarmstadt.ukp.dkpro.bigdata.io.hadoop.CASWritable;
import de.tudarmstadt.ukp.dkpro.core.api.lexmorph.type.pos.POS;
import de.tudarmstadt.ukp.dkpro.core.api.segmentation.type.Lemma;
import de.tudarmstadt.ukp.dkpro.core.api.segmentation.type.Sentence;

/**
 * Pass 1 of the Collocation discovery job which generated ngrams and emits ngrams an their
 * component n-1grams. Input is a SequeceFile<Text,StringTuple>, where the key is a document id and
 * the value is the tokenized documents.
 * <p/>
 */
public class CollocMapper
    extends Mapper<Text, CASWritable, GramKey, Gram>
{

    private static final byte[] EMPTY = new byte[0];

    public static final String MAX_SHINGLE_SIZE = "maxShingleSize";

    private static final int DEFAULT_MAX_SHINGLE_SIZE = 2;

    public enum Count
    {
        NGRAM_TOTAL, OVERFLOW, MULTIWORD, EMITTED_UNIGRAM, SENTENCES, LEMMA, DOCSIZE, EMPTYDOC
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

    /**
     * Collocation finder: pass 1 map phase.
     * <p/>
     * Receives a token stream which gets passed through a Lucene ShingleFilter. The ShingleFilter
     * delivers ngrams of the appropriate size which are then decomposed into head and tail subgrams
     * which are collected in the following manner
     * <p/>
     * 
     * <pre>
     * k:head_key,           v:head_subgram
     * k:head_key,ngram_key, v:ngram
     * k:tail_key,           v:tail_subgram
     * k:tail_key,ngram_key, v:ngram
     * </pre>
     * <p/>
     * The 'head' or 'tail' prefix is used to specify whether the subgram in question is the head or
     * tail of the ngram. In this implementation the head of the ngram is a (n-1)gram, and the tail
     * is a (1)gram.
     * <p/>
     * For example, given 'click and clack' and an ngram length of 3:
     * 
     * <pre>
     * k: head_'click and'                         v:head_'click and'
     * k: head_'click and',ngram_'click and clack' v:ngram_'click and clack'
     * k: tail_'clack',                            v:tail_'clack'
     * k: tail_'clack',ngram_'click and clack'     v:ngram_'click and clack'
     * </pre>
     * <p/>
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

            int count = 0;
            if (this.windowMode == Window.DOCUMENT)
                count = extractWholeDocument(context, jcas, lemmaCount, count);
            else if (this.windowMode == Window.SENTENCE)

                count = extractSentence(context, jcas, lemmaCount, count);
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
            context.getCounter(Count.NGRAM_TOTAL).increment(count);

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

    private int extractSentence(final Context context, final JCas jcas, int lemmaCount, int count)
    {
        OpenObjectIntHashMap<String> ngrams = new OpenObjectIntHashMap<String>(lemmaCount * 4);
        OpenObjectIntHashMap<String> unigrams = new OpenObjectIntHashMap<String>(lemmaCount);
        int sentenceCount = 0;

        Annotation[] previous = new Annotation[window];
        for (final Annotation sentence : select(jcas, Sentence.class)) {
            for (int j = 0; j < previous.length - 1; j++)
                previous[j] = previous[j + 1];
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

    private int extractWholeDocument(final Context context, final JCas jcas, int lemmaCount,
            int count)
    {
        OpenObjectIntHashMap<String> ngrams = new OpenObjectIntHashMap<String>(lemmaCount * 4);
        OpenObjectIntHashMap<String> unigrams = new OpenObjectIntHashMap<String>(lemmaCount);
        int sentenceCount = 0;
        count += collectCooccurencesFromCas(context, jcas, ngrams, unigrams);

        flushCollocations(context, ngrams, unigrams);
        context.getCounter(Count.SENTENCES).increment(sentenceCount);
        context.getCounter(Count.NGRAM_TOTAL).increment(count);
        return count;
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

    private int collectCooccurencesFromCasPOS(final Context context, final JCas jcas,
            OpenObjectIntHashMap<String> ngrams, OpenObjectIntHashMap<String> unigrams)
    {
        int count = 0;
        for (final Sentence sentence : select(jcas, Sentence.class)) {

            int i = 0;
            int pointer = -1;
            String[] history1 = new String[3];
            for (final Lemma term : JCasUtil.selectCovered(jcas, Lemma.class, sentence)) {
                final String termText = term.getValue().toLowerCase();
                for (int j = 0; j < history1.length - 1; j++)
                    history1[j] = history1[j + 1];
                history1[history1.length - 1] = termText;
                String mwe = StaticMultiWords.matches(history1);
                if (mwe != null) {
                    context.getCounter(Count.MULTIWORD).increment(1);
                    mwe = mwe + "/MWE";
                    String[] history2 = new String[3];
                    unigrams.adjustOrPutValue(mwe, 1, 1);
                    for (final Lemma term2 : JCasUtil.selectCovered(jcas, Lemma.class, sentence)) {
                        final String termText2 = term2.getValue().toLowerCase();
                        for (int j = 0; j < history2.length - 1; j++)
                            history2[j] = history2[j + 1];
                        history2[history1.length - 1] = termText2;

                        // // out.set(termText, termText2);
                        // ngrams.adjustOrPutValue(termText+" "+termText2, 1, 1);
                        // count++;
                        if (!isValid(termText2)) {
                            continue;
                        }
                        POS pos2 = null;
                        for (POS p : JCasUtil.selectCovered(jcas, POS.class, term2))
                            pos2 = p;

                        String posValue = pos2.getPosValue();
                        if (posValue.length() > 2)
                            posValue = posValue.substring(0, 2);
                        ngrams.adjustOrPutValue(mwe + "\t" + termText2 + "/" + posValue, 1, 1);
                        String mwe2 = StaticMultiWords.matches(history2);
                        if (mwe2 != null)
                            ngrams.adjustOrPutValue(mwe + "\t" + mwe2 + "/mwe", 1, 1);

                        count++;
                    }

                }
                POS pos = null;
                for (POS p : JCasUtil.selectCovered(jcas, POS.class, term))
                    pos = p;

                if (!isValid(termText)) {

                    continue;
                }
                String posValue = pos.getPosValue();
                if (posValue.length() > 2)
                    posValue = posValue.substring(0, 2);

                String left = termText + "/" + posValue;
                unigrams.adjustOrPutValue(left, 1, 1);
                for (final Lemma term2 : JCasUtil.selectCovered(jcas, Lemma.class, sentence)) {
                    final String termText2 = term2.getValue().toLowerCase();
                    // // out.set(termText, termText2);
                    // ngrams.adjustOrPutValue(termText+" "+termText2, 1, 1);
                    // count++;
                    if (!isValid(termText2)) {
                        continue;
                    }
                    POS pos2 = null;
                    for (POS p : JCasUtil.selectCovered(jcas, POS.class, term2))
                        pos2 = p;
                    posValue = pos2.getPosValue();
                    if (posValue.length() > 2)
                        posValue = posValue.substring(0, 2);

                    ngrams.adjustOrPutValue(left + "\t" + termText2 + "/" + posValue, 1, 1);
                    count++;

                }
                if (i++ > 5000) {
                    context.getCounter(Count.OVERFLOW).increment(1);
                    break;
                }

            }

        }
        return count;
    }

    private int collectCooccurencesFromCas(final Context context, JCas jcas,
            OpenObjectIntHashMap<String> ngrams, OpenObjectIntHashMap<String> unigrams)
    {
        int count = 0;

        int i = 0;

        String[] history1 = new String[5];

        for (final Lemma term : JCasUtil.select(jcas, Lemma.class)) {
            final String termText = term.getValue().toLowerCase();
            // handle multiword expressions
            for (int j = 0; j < history1.length - 1; j++)
                history1[j] = history1[j + 1];
            history1[history1.length - 1] = termText;

            String mwe = StaticMultiWords.matches(history1);
            if (mwe != null) {
                context.getCounter(Count.MULTIWORD).increment(1);
                String[] history2 = new String[5];
                unigrams.adjustOrPutValue(mwe, 1, 1);
                for (final Lemma term2 : JCasUtil.select(jcas, Lemma.class)) {
                    final String termText2 = term2.getValue().toLowerCase();
                    for (int j = 0; j < history2.length - 1; j++)
                        history2[j] = history2[j + 1];
                    history2[history1.length - 1] = termText2;

                    // // out.set(termText, termText2);
                    // ngrams.adjustOrPutValue(termText+" "+termText2, 1, 1);
                    // count++;
                    if (!isValid(termText2)) {
                        continue;
                    }

                    ngrams.adjustOrPutValue(mwe + "\t" + termText2, 1, 1);
                    String mwe2 = StaticMultiWords.matches(history2);
                    if (mwe2 != null)
                        ngrams.adjustOrPutValue(mwe + "\t" + mwe2, 1, 1);

                    count++;
                }

            }

            if (!isValid(termText)) {

                continue;
            }

            String left = termText;
            unigrams.adjustOrPutValue(left, 1, 1);
            for (final Lemma term2 : JCasUtil.select(jcas, Lemma.class)) {
                final String termText2 = term2.getValue().toLowerCase();
                // // out.set(termText, termText2);
                // ngrams.adjustOrPutValue(termText+" "+termText2, 1, 1);
                // count++;
                if (!isValid(termText2)) {
                    continue;
                }
                if (!left.equals(termText2))
                    ngrams.adjustOrPutValue(left + "\t" + termText2, 1, 1);
                count++;

            }
            if (i++ > 10000) {
                context.getCounter(Count.OVERFLOW).increment(1);
                return count;
            }

        }

        return count;
    }

    private int collectCooccurencesFromCoveringAnnotation(final Context context, JCas jcas,
            final Annotation sentence, OpenObjectIntHashMap<String> ngrams,
            OpenObjectIntHashMap<String> unigrams)
    {
        int count = 0;

        int i = 0;

        String[] history1 = new String[5];
        // for (Annotation sentence : sentence3)
        if (sentence != null)
            for (final Lemma term : JCasUtil.selectCovered(jcas, Lemma.class, sentence)) {
                final String termText = term.getValue().toLowerCase();
                // handle multiword expressions
                for (int j = 0; j < history1.length - 1; j++)
                    history1[j] = history1[j + 1];
                history1[history1.length - 1] = termText;

                String mwe = StaticMultiWords.matches(history1);
                if (mwe != null) {
                    context.getCounter(Count.MULTIWORD).increment(1);
                    String[] history2 = new String[5];
                    unigrams.adjustOrPutValue(mwe, 1, 1);
                    // for (Annotation sentence2 : sentence3)
                    if (sentence != null)
                        for (final Lemma term2 : JCasUtil
                                .selectCovered(jcas, Lemma.class, sentence)) {
                            final String termText2 = term2.getValue().toLowerCase();
                            for (int j = 0; j < history2.length - 1; j++)
                                history2[j] = history2[j + 1];
                            history2[history1.length - 1] = termText2;

                            // // out.set(termText, termText2);
                            // ngrams.adjustOrPutValue(termText+" "+termText2, 1, 1);
                            // count++;
                            if (!isValid(termText2)) {
                                continue;
                            }

                            ngrams.adjustOrPutValue(mwe + "\t" + termText2, 1, 1);
                            String mwe2 = StaticMultiWords.matches(history2);
                            if (mwe2 != null)
                                ngrams.adjustOrPutValue(mwe + "\t" + mwe2, 1, 1);

                            count++;
                        }

                }

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
                    if (!left.equals(termText2))
                        ngrams.adjustOrPutValue(left + "\t" + termText2, 1, 1);
                    count++;

                }
                if (i++ > 100) {
                    context.getCounter(Count.OVERFLOW).increment(1);
                    return count;
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
        try {

            this.metadata = aParser.parseResourceMetaData(new XMLInputSource(new StringInputStream(
                    Metadata.getMetadata()), new File(".")));
        }
        catch (final InvalidXMLException e1) {
            // TODO Auto-generated catch block
            e1.printStackTrace();
        }

        if (log.isInfoEnabled()) {
            // log.info("Max Ngram size is {}", this.maxShingleSize);
            log.info("Emit Unitgrams is {}", emitUnigrams);
            log.info("Window Mode is {}", this.windowMode.name());
            log.info("Window Size is {}", window);
            log.info("Emit Unitgrams is {}", emitUnigrams);

        }
    }
}
