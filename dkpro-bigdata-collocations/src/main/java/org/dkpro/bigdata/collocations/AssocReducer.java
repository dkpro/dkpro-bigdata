package org.dkpro.bigdata.collocations;

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

import java.io.IOException;
import java.lang.reflect.Method;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.mahout.math.stats.LogLikelihood;
import org.dkpro.bigdata.collocations.CollocMapper.Count;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Reducer for pass 2 of the collocation discovery job. Collects ngram and
 * sub-ngram frequencies and performs the Log-likelihood ratio calculation.
 */
public class AssocReducer extends Reducer<Gram, Gram, Text, DoubleWritable> {

	/** Counter to track why a particlar entry was skipped */
	public enum Skipped {
		EXTRA_HEAD, EXTRA_TAIL, MISSING_HEAD, MISSING_TAIL, LESS_THAN_MIN_VALUE, LLR_CALCULATION_ERROR, CHI_CALCULATION_ERROR, PMI_CALCULATION_ERROR, DICE_CALCULATION_ERROR
	}

	private static final Logger log = LoggerFactory
			.getLogger(AssocReducer.class);

	public static final String NGRAM_TOTAL = "ngramTotal";
	public static final String MIN_VALUE = "minLLR";
	public static final float DEFAULT_MIN_VALUE = 0.1f;
	public static final String ASSOC_METRIC = "metric";

	public static final String DEFAULT_ASSOC = "llr";

	private long ngramTotal;
	private float minValue;
	private boolean emitUnigrams;
	private AssocCallback assocCalculator;
	private final AssocCallback llrCalculator = new ConcreteLLCallback();
	private final AssocCallback pmiCalculator = new PMICallback();
	private final AssocCallback chiCalculator = new ChiSquareCallback();
	private final AssocCallback diceCalculator = new DiceCallback();
	private Method metricMethod;
	private MultipleOutputs<?, ?> mos;
	AssociationMetrics ass = new AssociationMetrics();

	/**
	 * Perform assoc calculation, input is: k:ngram:ngramFreq
	 * v:(h_|t_)subgram:subgramfreq N = ngram total
	 * 
	 * Each ngram will have 2 subgrams, a head and a tail, referred to as A and
	 * B respectively below.
	 * 
	 * A+ B: number of times a+b appear together: ngramFreq A+!B: number of
	 * times A appears without B: hSubgramFreq - ngramFreq !A+ B: number of
	 * times B appears without A: tSubgramFreq - ngramFreq !A+!B: number of
	 * times neither A or B appears (in that order): N - (subgramFreqA +
	 * subgramFreqB - ngramFreq)
	 */
	@Override
	protected void reduce(Gram ngram, Iterable<Gram> values, Context context)
			throws IOException, InterruptedException {

		int[] gramFreq = { -1, -1 };

		int frequency = ngram.getFrequency();
		if (ngram.getType() == Gram.Type.UNIGRAM && emitUnigrams) {
			DoubleWritable dd = new DoubleWritable(frequency);
			Text t = new Text(ngram.getString());
			context.getCounter(Count.EMITTED_UNIGRAM).increment(1);
			context.write(t, dd);
			return;
		}
		// TODO better way to handle errors? Wouldn't an exception thrown here
		// cause hadoop to re-try the job?
		String[] gram = new String[2];
		for (Gram value : values) {

			int pos = value.getType() == Gram.Type.HEAD ? 0 : 1;

			if (gramFreq[pos] != -1) {
				log.warn("Extra {} for {}, skipping", value.getType(), ngram);
				if (value.getType() == Gram.Type.HEAD) {
					context.getCounter(Skipped.EXTRA_HEAD).increment(1);
				} else {
					context.getCounter(Skipped.EXTRA_TAIL).increment(1);
				}
				return;
			}

			gram[pos] = value.getString();
			gramFreq[pos] = value.getFrequency();
		}

		if (gramFreq[0] == -1) {
			log.warn("Missing head for {}, skipping.", ngram);
			context.getCounter(Skipped.MISSING_HEAD).increment(1);
			return;
		}
		if (gramFreq[1] == -1) {
			log.warn("Missing tail for {}, skipping", ngram);
			context.getCounter(Skipped.MISSING_TAIL).increment(1);
			return;
		}

		double value;
		// build continguency table
		long k11 = frequency; /* a&b */
		long k12 = gramFreq[0] - frequency; /* a&!b */
		long k21 = gramFreq[1] - frequency; /* !b&a */
		long k22 = ngramTotal - (gramFreq[0] + gramFreq[1] - frequency); /* !a&!b */

		try {

			value = assocCalculator.assoc(k11, k12, k21, k22);

		} catch (IllegalArgumentException ex) {
			context.getCounter(Skipped.LLR_CALCULATION_ERROR).increment(1);
			log.warn(
					"Problem calculating assoc metric for ngram {}, HEAD {}:{}, TAIL {}:{}, k11/k12/k21/k22: {}/{}/{}/{}",
					new Object[] { ngram, gram[0], gramFreq[0], gram[1],
							gramFreq[1] }, ex);
			return;
		}
		if (value < minValue) {
			context.getCounter(Skipped.LESS_THAN_MIN_VALUE).increment(1);
		} else {

			ass.init(k11, k12, k21, k22);
			// try {
			// Object invoke = metricMethod.invoke(value, gram);
			// } catch (IllegalArgumentException e1) {
			// // TODO Auto-generated catch block
			// e1.printStackTrace();
			// } catch (IllegalAccessException e1) {
			// // TODO Auto-generated catch block
			// e1.printStackTrace();
			// } catch (InvocationTargetException e1) {
			// // TODO Auto-generated catch block
			// e1.printStackTrace();
			// }
			mos.write("llr", new Text(ngram.getString()), new DoubleWritable(
					value));
			try {
				double pmi = ass.mutual_information();// pmiCalculator.assoc(k11,
														// k12, k21, k22);
				mos.write("pmi", new Text(ngram.getString()),
						new DoubleWritable(pmi));
			} catch (Exception e) {
				context.getCounter(Skipped.PMI_CALCULATION_ERROR).increment(1);

			}
			try {

				double chi = ass.chisquared();// chiCalculator.assoc(k11, k12,
												// k21, k22);
				mos.write("chi", new Text(ngram.getString()),
						new DoubleWritable(chi));
			} catch (Exception e) {
				context.getCounter(Skipped.CHI_CALCULATION_ERROR).increment(1);

			}
			try {

				double dice = ass.dice();// diceCalculator.assoc(k11, k12, k21,
											// k22);
				mos.write("dice", new Text(ngram.getString()),
						new DoubleWritable(dice));
			} catch (Exception e) {
				context.getCounter(Skipped.DICE_CALCULATION_ERROR).increment(1);

			}

			context.getCounter("assoctest", "EMITTED NGRAM").increment(1);

			mos.write("contingency", new Text(ngram.getString()), new Text(""
					+ k11 + "\t" + k12 + "\t" + k21 + "\t" + k22));

		}
	}

	@Override
	protected void cleanup(Context context) throws IOException,
			InterruptedException {
		mos.close();
	}

	@Override
	protected void setup(Context context) throws IOException,
			InterruptedException {
		super.setup(context);
		Configuration conf = context.getConfiguration();
		this.ngramTotal = conf.getLong(NGRAM_TOTAL, -1);
		this.minValue = conf.getFloat(MIN_VALUE, DEFAULT_MIN_VALUE);
		String assocType = conf.get(ASSOC_METRIC, DEFAULT_ASSOC);
		if (assocType.equalsIgnoreCase("llr"))
			assocCalculator = new ConcreteLLCallback();
		else if (assocType.equalsIgnoreCase("dice"))
			assocCalculator = new DiceCallback();
		else if (assocType.equalsIgnoreCase("pmi"))
			assocCalculator = new PMICallback();
		else if (assocType.equalsIgnoreCase("chi"))
			assocCalculator = new ChiSquareCallback();

		this.emitUnigrams = conf.getBoolean(CollocDriver.EMIT_UNIGRAMS,
				CollocDriver.DEFAULT_EMIT_UNIGRAMS);
		log.info("NGram Total: {}, Min DICE value: {}, Emit Unigrams: {}",
				new Object[] { ngramTotal, minValue, emitUnigrams });

		if (ngramTotal == -1) {
			throw new IllegalStateException(
					"No NGRAM_TOTAL available in job config");
		}
		mos = new MultipleOutputs<Text, DoubleWritable>(context);
	}

	public AssocReducer() {
		this.assocCalculator = new DiceCallback();
	}

	/**
	 * plug in an alternate LL implementation, used for testing
	 * 
	 * @param ll
	 *            the LL to use.
	 */
	AssocReducer(AssocCallback ll) {
		this.assocCalculator = ll;
	}

	/**
	 * provide interface so the input to the DICE calculation can be captured
	 * for validation in unit testing
	 */
	public interface AssocCallback {
		double assoc(long k11, long k12, long k21, long k22);
	}

	/** concrete implementation delegates to LogLikelihood class */
	public static final class ConcreteLLCallback implements AssocCallback {
		@Override
		public double assoc(long k11, long k12, long k21, long k22) {

			return LogLikelihood.logLikelihoodRatio(k11, k12, k21, k22);
		}
	}

	public static final class DiceCallback implements AssocCallback {

		@Override
		public double assoc(long k11, long k12, long k21, long k22) {
			return (2.0 * k11) / (2 * k11 + k12 + k21);
		}

	}

	public static final class PMICallback implements AssocCallback {

		@Override
		public double assoc(long k11, long k12, long k21, long k22) {
			double total = k11 + k12 + k21 + k22;
			// expected values :
			double e11 = (k11 + k12) * (k11 + k21) / total;
			double e12 = (k12 + k11) * (k12 + k22) / total;
			double e21 = (k21 + k22) * (k21 + k11) / total;
			// double e22 = (double) ((k22 + k21) * (k22 + k12)) / (double)
			// total;

			// PMI is log_2(P(a|b)/P(a)*P(b))
			// ngramfreq/total/(b/total)
			return Math.log(e11 / (e12) * (e21)) / Math.log(2);
			// return Math.log(k11 * k22 / (k11+k21 * k11+k12)) / Math.log(2);
		}
	}

	public static final class ChiSquareCallback implements AssocCallback {

		@Override
		public double assoc(long k11, long k12, long k21, long k22) {
			double n = k11 + k12 + k21 + k22;
			return (n * (k11 * k22 - k21 * k12) / ((k11 + k21) * (k12 + k22)
					* (k11 + k12) * (k21 + k22)));
		}
	}

}
