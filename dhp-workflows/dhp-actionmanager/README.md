# Action Management Framework

This module implements the oozie workflow for the integration of pre-built contents into the OpenAIRE Graph.

Such contents can be 

* brand new, non-existing records to be introduced as nodes of the graph
* updates (or enrichment) for records that does exist in the graph (e.g. a new subject term for a publication)
* relations among existing nodes

The actionset contents are organised into logical containers, each of them can contain multiple versions contents and is characterised by

* a name
* an identifier
* the paths on HDFS where each version of the contents is stored

Each version is then characterised by 

* the creation date
* the last update date
* the indication where it is the latest one or it is an expired version, candidate for garbage collection

## ActionSet serialization

Each actionset version contains records compliant to the graph internal data model, i.e. subclasses of `eu.dnetlib.dhp.schema.oaf.Oaf`,
defined in the external schemas module

```
<dependency>
    <groupId>eu.dnetlib.dhp</groupId>
    <artifactId>${dhp-schemas.artifact}</artifactId>
    <version>${dhp-schemas.version}</version>
</dependency>
```

When the actionset contains a relationship, the model class to use is `eu.dnetlib.dhp.schema.oaf.Relation`, otherwise 
when the actionset contains an entity, it is a `eu.dnetlib.dhp.schema.oaf.OafEntity` or one of its subclasses 
`Datasource`, `Organization`, `Project`, `Result` (or one of its subclasses `Publication`, `Dataset`, etc...). 

Then, each OpenAIRE Graph model class instance must be wrapped using the class `eu.dnetlib.dhp.schema.action.AtomicAction`, a generic 
container that defines two attributes

* `T payload` the OpenAIRE Graph class instance containing the data;
* `Class<T> clazz` must contain the class whose instance is contained in the payload.

Each AtomicAction can be then serialised in JSON format using `com.fasterxml.jackson.databind.ObjectMapper` from

```
<dependency>
    <groupId>com.fasterxml.jackson.core</groupId>
    <artifactId>jackson-databind</artifactId>
    <version>${dhp.jackson.version}</version>
</dependency>
```

Then, the JSON serialization must be stored as a GZip compressed sequence file (`org.apache.hadoop.mapred.SequenceFileOutputFormat`). 
As such, it contains a set of tuples, a key and a value defined as `org.apache.hadoop.io.Text` where

* the `key` must be set to the class canonical name contained in the `AtomicAction`;
* the `value` must be set to the AtomicAction JSON serialization.

The following snippet provides an example of how create an actionset version of Relation records:

```
  rels // JavaRDD<Relation>
    .map(relation -> new AtomicAction<Relation>(Relation.class, relation))
    .mapToPair(
        aa -> new Tuple2<>(new Text(aa.getClazz().getCanonicalName()),
            new Text(OBJECT_MAPPER.writeValueAsString(aa))))
    .saveAsHadoopFile(outputPath, Text.class, Text.class, SequenceFileOutputFormat.class, GzipCodec.class);
```

