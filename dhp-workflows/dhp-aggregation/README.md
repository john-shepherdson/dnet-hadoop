Description of the Module
--------------------------
This module defines a set of oozie workflows for the **collection** and **transformation** of metadata records.
Both workflows interact with the Metadata Store Manager (MdSM) to handle the logical transactions required to ensure
the consistency of the read/write operations on the data as the MdSM in fact keeps track of the logical-physical mapping 
of each MDStore.

## Metadata collection

The **metadata collection workflow** is responsible for harvesting metadata records from different protocols and responding to 
different formats and to store them as on HDFS so that they can be further processed. 

### Collector Plugins

Different protocols are managed by dedicated Collector plugins, i.e. java programs implementing a defined interface:

```eu.dnetlib.dhp.collection.plugin.CollectorPlugin```

The list of the supported plugins:

* OAI Plugin: collects from OAI-PMH compatible endpoints
* MDStore plugin: collects from a given D-Net MetadataStore, (identified by moogodb URI, dbName, MDStoreID)
* MDStore dump plugin: collects from an MDStore dump stored on the HDFS location indicated by the `path` parameter 

# Transformation Plugins
TODO

