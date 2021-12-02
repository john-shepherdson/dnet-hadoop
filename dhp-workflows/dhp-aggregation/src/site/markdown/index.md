##DHP-Aggregation

This module defines a set of oozie workflows for

1. the **collection** and **transformation** of metadata records.
2. the **integration** of new external information in the result


### Collection and Transformation

The workflows interact with the Metadata Store Manager (MdSM) to handle the logical transactions required to ensure
the consistency of the read/write operations on the data as the MdSM in fact keeps track of the logical-physical mapping
of each MDStore.

It defines [mappings](mappings.md) for transformation of different datasource (See mapping section).

### Integration of external information in the result

The workflows create new entity in the OpenAIRE format (OAF) whose aim is to enrich the result already contained in the graph.
See integration section for more insight
