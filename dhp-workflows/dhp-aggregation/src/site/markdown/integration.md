DHP Aggregation - Integration method
=====================================

The integration method can be applied every time new information, which is not aggregated from the repositories
nor computed directly by OpenAIRE, should be added to the results of the graph.

The information integrated so far is:

1. Article impact measures
    1. [Bip!Finder](https://dl.acm.org/doi/10.1145/3357384.3357850) scores
2. Result Subjects
    1. Integration of Fields of Science and Techonology ([FOS](https://www.qnrf.org/en-us/FOS))  classification in
    results subjects.


The method always consists in the creation of a new entity in the OpenAIRE format (OAF entity) containing only the id
and the element in the OAF model that should be used to map the information we want to integrate.

The id is set by using a particular encoding of the given PID

*unresolved::[pid]::[pidtype]*

where

1. *unresolved* is a constant value
2. *pid*  is the persistent id value, e.g. 10.5281/zenodo.4707307
3. *pidtype* is the persistent id type, e.g. doi

Such entities are matched against those available in the graph using the result.instance.pid values.

This mechanism can be used to integrate enrichments produced as associated by a given PID.
If a match will be found with one of the results already in the graph that said result will be enriched with the information
present in the new OAF.
All the entities for which a match is not found are discarded.


