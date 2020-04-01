package eu.dnetlib.dhp.oa.provision;

import eu.dnetlib.dhp.application.ArgumentApplicationParser;
import eu.dnetlib.dhp.oa.provision.model.*;
import eu.dnetlib.dhp.oa.provision.utils.ContextMapper;
import eu.dnetlib.dhp.schema.oaf.*;
import org.apache.commons.io.IOUtils;
import org.apache.spark.SparkConf;
import org.apache.spark.sql.SparkSession;

public class SparkXmlRecordBuilderJob_v2 {

    public static void main(String[] args) throws Exception {

        final ArgumentApplicationParser parser = new ArgumentApplicationParser(
                IOUtils.toString(
                        SparkXmlRecordBuilderJob_v2.class.getResourceAsStream("/eu/dnetlib/dhp/oa/provision/input_params_build_adjacency_lists.json")));
        parser.parseArgument(args);

        final String master = parser.get("master");
        try(SparkSession spark = getSession(master)) {

            final String inputPath = parser.get("sourcePath");
            final String outputPath = parser.get("outputPath");
            final String isLookupUrl = parser.get("isLookupUrl");
            final String otherDsTypeId = parser.get("otherDsTypeId");

            new GraphJoiner_v2(spark, ContextMapper.fromIS(isLookupUrl), otherDsTypeId, inputPath, outputPath)
                    .adjacencyLists();
        }
    }

    private static SparkSession getSession(String master) {
        final SparkConf conf = new SparkConf();
        conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer");
        conf.set("spark.sql.shuffle.partitions", "500");
        conf.registerKryoClasses(new Class[]{
                Author.class,
                Context.class,
                Country.class,
                DataInfo.class,
                eu.dnetlib.dhp.schema.oaf.Dataset.class,
                Datasource.class,
                ExternalReference.class,
                ExtraInfo.class,
                Field.class,
                GeoLocation.class,
                Instance.class,
                Journal.class,
                KeyValue.class,
                Oaf.class,
                OafEntity.class,
                OAIProvenance.class,
                Organization.class,
                OriginDescription.class,
                OtherResearchProduct.class,
                Project.class,
                Publication.class,
                Qualifier.class,
                Relation.class,
                Result.class,
                Software.class,
                StructuredProperty.class,

                TypedRow.class,
                EntityRelEntity.class,
                JoinedEntity.class,
                SortableRelationKey.class,
                Tuple2.class,
                Links.class,
                RelatedEntity.class
        });
        return SparkSession
                .builder()
                .config(conf)
                .appName(SparkXmlRecordBuilderJob_v2.class.getSimpleName())
                .master(master)
                .getOrCreate();
    }

}
