# Introduction #

Collocation Mining is base for a range of NLP applications
  * Mining Multiword-Expressions
  * Lexicon Construction (cf JobImText)
  * Word Sense Induction

DKPro supports two methods of doing this. Firstly, we include a hacked
up version of the collocation extraction from Apache Mahout. The main changes are:
  * Instead of seq2sparse input, we use !CASWritables
  * You can specify the context window in terms of UIMA Annotations
  * You can specify the Items to be analyzed in terms of UIMA Annotations
  * Besides LLR, we support a range of association metrics as found at http://www.collocations.de

# Usage #

```


Usage:                                                                          
 [--input <input> --output <output> --maxNGramSize <ngramSize> --overwrite      
--minSupport <minSupport> --minLLR <minLLR> --numReducers <numReducers>         
--analyzerName <analyzerName> --preprocess --unigram --help]                    
Options                                                                         
  --input (-i) input                  The Path for input files.                 
  --output (-o) output                The Path write output to                  
  --overwrite (-w)                    If set, overwrite the output directory    
  --minSupport (-s) minSupport        (Optional) Minimum Support. Default       
                                      Value: 2                                 
  --minLLR (-ml) minLLR               (Optional)The minimum Log Likelihood      
                                      Ratio(Float)  Default is 1.0             
  --numReducers (-nr) numReducers     (Optional) Number of reduce tasks.        
                                      Default Value: 1                         
  --unigram (-u)                      If set, unigrams will be emitted in the   
                                      final output alongside collocations       
  --help (-h)                         Print out help 
  --window
    
            
```
## Using WebCorpus or JoBimText ##
The second way of  generating ngrams from a corpus is provided by the WebCorpus project.

The JoBimText project provides a generalization of collocations called "holing".