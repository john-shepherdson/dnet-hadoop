package eu.dnetlib.dhp.bulktag.resolver;

import eu.dnetlib.dhp.bulktag.community.Constraint;
import eu.dnetlib.dhp.bulktag.community.Constraints;
import eu.dnetlib.dhp.bulktag.community.TaggingConstraint;
import eu.dnetlib.dhp.bulktag.community.TaggingConstraints;
import eu.dnetlib.dhp.bulktag.criteria.Selection;
import eu.dnetlib.dhp.bulktag.criteria.VerbResolver;
import eu.dnetlib.dhp.bulktag.criteria.VerbResolverFactory;
import eu.dnetlib.dhp.schema.oaf.Result;
import org.apache.spark.api.java.function.FilterFunction;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.*;
import org.codehaus.jackson.map.ObjectMapper;
import static org.apache.spark.sql.functions.*;


import java.beans.IntrospectionException;
import java.beans.PropertyDescriptor;
import java.io.IOException;
import java.io.Serializable;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;

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

    private <R extends Result>  void evaluate(SparkSession spark, TaggingConstraint tag) throws ClassNotFoundException {
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
                    .select(left.col("*"), right.col(tag.getRelatedEntity().getField())); // selezioni solo quello che ti serve

//            Column[] leftCols = Arrays.stream(result.columns())
//                    .filter(c -> !c.equals("startDate"))
//                    .map(functions::col)
//                    .toArray(Column[]::new);
//
            // Dataset<Row> result già joinato
            Dataset<Row> jsonDataset = result
                    // JSON della parte left (tutte le colonne tranne startDate)
                    .withColumn("left", to_json(struct(Arrays.stream(result.columns())
                            .filter(c -> !c.equals(tag.getRelatedEntity().getField()))
                            .map(functions::col)
                            .toArray(Column[]::new))))
                    // JSON della parte right (solo startDate)
                    .withColumn("right", to_json(struct(col(tag.getRelatedEntity().getField()))));

            jsonDataset.map((MapFunction<Row, String>)e  -> {
                String leftJson = e.getAs("left");
                String rightJson = e.getAs("right");

                if(rightJson == null)
                    return leftJson;
                if(tag.getCriteria().stream().anyMatch(c -> c.getConstraint().stream().allMatch(con -> {
                    ObjectMapper mapper = new ObjectMapper();
                    String className = tag.getEntityClass();
                    Class<?> clazz = null;
                    try {
                        clazz = Class.forName(className);
                    } catch (ClassNotFoundException ex) {
                        throw new RuntimeException(ex);
                    }
                    try {
                        Object leftInstance = mapper.readValue(leftJson, clazz);
                        String fieldName =  con.getField();
                        PropertyDescriptor pd = new PropertyDescriptor(fieldName, leftInstance.getClass());
                        Object value = pd.getReadMethod().invoke(leftInstance);
                        return con.verifyCriteria(value, rightJson);
                    } catch (IOException ex) {
                        throw new RuntimeException(ex);
                    } catch (IntrospectionException ex) {
                        throw new RuntimeException(ex);
                    } catch (InvocationTargetException ex) {
                        throw new RuntimeException(ex);
                    } catch (IllegalAccessException ex) {
                        throw new RuntimeException(ex);
                    }

                }))){
                    ObjectMapper mapper = new ObjectMapper();
                    String className = tag.getEntityClass();
                    Class<?> clazz = null;
                    try {
                        clazz = Class.forName(className);
                    } catch (ClassNotFoundException ex) {
                        throw new RuntimeException(ex);
                    }
                    try {
                        Object leftInstance = mapper.readValue(leftJson, clazz);
                        PropertyDescriptor pd = new PropertyDescriptor("id", leftInstance.getClass());
                        return pd.getReadMethod().invoke(leftInstance) + "@@" + tag.getId();
                    } catch (IOException ex) {
                        throw new RuntimeException(ex);
                    } catch (IntrospectionException ex) {
                        throw new RuntimeException(ex);
                    } catch (InvocationTargetException ex) {
                        throw new RuntimeException(ex);
                    } catch (IllegalAccessException ex) {
                        throw new RuntimeException(ex);
                    }
                }

                return null;
            }, Encoders.STRING())
                    .filter(Objects::nonNull);

        }

    }


}
