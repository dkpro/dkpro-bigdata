# Introduction #

This package is a collection of pig and bash scripts that create vectors
of top-n cooccurences for a set of target words.

From those vectors, we can extract the rank of one word in respect to
the other and calculate a normalized similarity measure called RankRatio.

# Installation #

The package requires
  * a working hadoop installation, including apache pig.
  * [The DKPro Collocations package](DKProCollocations.md)
  * A corpus, in a DKPro BigData readable format
  * [PiggyBank](https://cwiki.apache.org/confluence/display/PIG/PiggyBank) and [DataFu](http://engineering.linkedin.com/datafu/datafu-10)




# Usage #