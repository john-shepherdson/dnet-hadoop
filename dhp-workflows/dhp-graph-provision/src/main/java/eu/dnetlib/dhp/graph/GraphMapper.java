package eu.dnetlib.dhp.graph;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import eu.dnetlib.dhp.schema.oaf.*;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SparkSession;

import java.io.IOException;
import java.util.stream.Collectors;

public class GraphMapper {


    public void map(final SparkSession spark, final String outPath) {

        final JavaSparkContext sc = new JavaSparkContext(spark.sparkContext());

        sc.textFile(outPath + "/linked_entities")
                .map(LinkedEntityWrapper::parse)
                .map(GraphMapper::asLinkedEntity)
                .map(e -> new ObjectMapper().writeValueAsString(e))
                .saveAsTextFile(outPath + "/linked_entities_types");
    }

    private static LinkedEntity asLinkedEntity(final LinkedEntityWrapper lw) throws JsonProcessingException {
        final LinkedEntity le = new LinkedEntity();

        try {
            le.setType(lw.getEntity().getType());
            le.setEntity(parseEntity(lw.getEntity().getOaf(), le.getType()));
            le.setLinks(lw.getLinks()
                    .stream()
                    .map(l -> new Link()
                            .setRelation(parseRelation(l.getRelation().getOaf()))
                            .setRelatedEntity(RelatedEntity.parse(l.getTarget().getOaf())))
                    .collect(Collectors.toList()));
            return le;
        } catch (IllegalArgumentException e) {
            throw new IllegalArgumentException(new ObjectMapper().writeValueAsString(lw), e);
        }
    }

    private static Relation parseRelation(final String s) {
        try {
            return new ObjectMapper().readValue(s, Relation.class);
        } catch (IOException e) {
            throw new IllegalArgumentException("unable to decode Relation: " + s);
        }
    }

    private static OafEntity parseEntity(final String json, final String type) {
        final ObjectMapper o = new ObjectMapper();
        try {
            switch (type) {
                case "publication":
                    return o.readValue(json, Publication.class);
                case "dataset":
                    return o.readValue(json, Dataset.class);
                case "otherresearchproduct":
                    return o.readValue(json, OtherResearchProduct.class);
                case "software":
                    return o.readValue(json, Software.class);
                case "datasource":
                    return o.readValue(json, Datasource.class);
                case "project":
                    return o.readValue(json, Project.class);
                case "organization":
                    return o.readValue(json, Organization.class);
                default:
                    throw new IllegalArgumentException("invalid entity type: " + type);
            }
        } catch (IOException e) {
            throw new IllegalArgumentException("unable to decode oaf entity: " + json);
        }
    }
}
