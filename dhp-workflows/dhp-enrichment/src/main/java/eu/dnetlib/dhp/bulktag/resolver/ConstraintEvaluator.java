package eu.dnetlib.dhp.bulktag.resolver;

import eu.dnetlib.dhp.bulktag.community.Constraint;
import eu.dnetlib.dhp.bulktag.community.Constraints;
import eu.dnetlib.dhp.bulktag.community.TaggingConstraint;
import eu.dnetlib.dhp.bulktag.community.TaggingConstraints;
import eu.dnetlib.dhp.bulktag.criteria.Selection;
import eu.dnetlib.dhp.bulktag.criteria.VerbResolver;
import eu.dnetlib.dhp.bulktag.criteria.VerbResolverFactory;
import org.apache.spark.api.java.function.FilterFunction;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.*;
import org.codehaus.jackson.map.ObjectMapper;
import static org.apache.spark.sql.functions.*;


import java.io.IOException;
import java.io.Serializable;
import java.util.Arrays;
import java.util.List;

public class ConstraintEvaluator implements Serializable {

    private final TaggingConstraints tags;

    public ConstraintEvaluator(String tagsDefinition) throws IOException {
        this.tags = new ObjectMapper().readValue(tagsDefinition, TaggingConstraints.class);
        this.tags.getTags().forEach(t -> {
            t.setSelection(VerbResolverFactory.newInstance());
            });

    }

    public void evaluate(SparkSession sparkSession){
        this.tags.getTags().forEach(t -> {
            try {
                evaluate(sparkSession, t);
            } catch (ClassNotFoundException e) {
                throw new RuntimeException(e);
            }
        });
    }

    private void evaluate(SparkSession spark, TaggingConstraint tag) throws ClassNotFoundException {
        if(tag.getRelatedEntity() != null){
            Dataset<Row> rels = spark.read()
                    .json(tags.getGraphPath() + "/relation")
                    .where("relClass = '" + tag.getRelatedEntity().getRelation() + "'")
                    .select(col("source"), col("target")); // parentesi corretta

            Dataset<Row> left = spark.read()
                    .json(tags.getGraphPath() + "/" + tag.getEntityToTag());

            Dataset<Row> right = spark.read()
                    .json(tags.getGraphPath() + "/" + tag.getRelatedEntity().getEntity());

// join left -> rels -> right
            Dataset<Row> result = left
                    .join(rels, left.col("id").equalTo(rels.col("source")), "left")
                    .join(right, right.col("id").equalTo(rels.col("target")), "left")
                    .select(left.col("*"), right.col("startDate")); // selezioni solo quello che ti serve

//            Column[] leftCols = Arrays.stream(result.columns())
//                    .filter(c -> !c.equals("startDate"))
//                    .map(functions::col)
//                    .toArray(Column[]::new);
//
            // Dataset<Row> result già joinato
            Dataset<Row> jsonDataset = result
                    // JSON della parte left (tutte le colonne tranne startDate)
                    .withColumn("left", to_json(struct(Arrays.stream(result.columns())
                            .filter(c -> !c.equals("startDate"))
                            .map(functions::col)
                            .toArray(Column[]::new))))
                    // JSON della parte right (solo startDate)
                    .withColumn("right", to_json(struct(col("startDate"))));

            jsonDataset.map((MapFunction<Row, String>)e  -> {
                String leftJson = e.getAs("left");
                String rightJson = e.getAs("right");

                if(rightJson == null)
                    return leftJson;
                if(tag.getCriteria().stream().anyMatch(c -> c.getConstraint().stream().allMatch(con -> con.verifyCriteria())))
                return null;
            }, Encoders.STRING());

        }

    }


}
