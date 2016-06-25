/*******************************************************************************
 * Copyright 2013
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
package org.dkpro.bigdata.collocations;

import org.apache.commons.lang.NotImplementedException;

/**
 * Implementation of association metrics from Stefan Everts' www.collocations.de
 * 
 * @author zorn
 * 
 */
public class AssociationMetrics
{
    // observed frequencies
    double o11;
    double o12;
    double o21;
    double o22;
    // marginals
    double r1;
    double r2;
    double c1;
    double c2;
    double n;
    // expected values
    double e11 = 0.0;
    double e12 = 0.0;
    double e21 = 0.0;
    double e22 = 0.0;
    // observed frequencies
    double O11;
    double O12;
    double O21;
    double O22;
    // marginals
    double R1;
    double R2;
    double C1;
    double C2;
    double N;

    public void init(long o11, long o12, long o21, long o22)
    {
        this.o11 = o11;
        this.o12 = o12;
        this.o21 = o21;
        this.o22 = o22;
        // calculate marginals
        R1 = o11 + o12;
        R2 = o21 + o22;
        C1 = o11 + o21;
        C2 = o12 + o22;
        N = R1 + R2;
        // pre-type convert, not sure whether this really is necessary, need to benchmark
        r1 = R1;
        r2 = R2;
        c1 = C1;
        c2 = C2;
        n = N;
        // calculate expected values;
        e11 = (r1 * c1) / N;
        e12 = (r1 * c2) / N;
        e21 = (r2 * c1) / N;
        e22 = (r2 * c2) / N;

    }

    public double pmi()
    {
        return Math.log(e11 / (e12) * (e21)) / Math.log(2);
    }

    public double chisquared()
    {
        return (N * (o11 - e11) * N * (o11 - e11)) / (e11 * e22);
    }

    public double chisquared_corr()
    {
        return ((N * (o11 * o22 - e11) * N * (o12 - o21)) - (n / 2)) / (r1 * r2 * c1 * c2);
    }

    public double mutual_information()
    {
        return Math.log(o11 / e11);
    }

    public double odds_ratio_discounted()
    {
        return ((o11 + 0.5) * (o22 + 0.5)) / ((o12 + 0.5) * (o21 + 0.5));
    }

    public double dice()
    {
        return (2 * o11 / (r1 + c1));
    }

    public double ms()
    {
        return Math.min(o11 / r1, o11 / c1);
    }

    public double gmean()
    {
        return o11 / Math.sqrt(N * e11);
    }

    public double local_mi()
    {
        return 0;
    }

    public double average_mi()
    {
        return 0;
    }

    // public double fisher() {
    // double sum=0;
    // for (long k=(long) o11;k<Math.max(r1,c1);k++) {
    // sum+=(binomial((long) c1,k)*binomial((long) c2,(long)r1-k))/binomial((long)N,(long)r1);
    // }
    // }
    //
    //
    // private double binomial(long n, long k)
    // {
    // // TODO Auto-generated method stub
    // return 0;
    // }

}
