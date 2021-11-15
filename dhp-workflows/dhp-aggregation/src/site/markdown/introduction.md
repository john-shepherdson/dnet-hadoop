##DHP-Aggregation

This module defines a set of oozie workflows for the **collection** and **transformation** of metadata records.

Both workflows interact with the Metadata Store Manager (MdSM) to handle the logical transactions required to ensure
the consistency of the read/write operations on the data as the MdSM in fact keeps track of the logical-physical mapping
of each MDStore.