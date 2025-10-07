package eu.dnetlib.dhp.bulktag.resolver;

import com.jayway.jsonpath.JsonPath;
import com.jayway.jsonpath.ReadContext;
import eu.dnetlib.dhp.bulktag.community.TaggingConstraint;
import eu.dnetlib.dhp.bulktag.community.TaggingConstraints;
import eu.dnetlib.dhp.bulktag.criteria.VerbResolverFactory;
import eu.dnetlib.dhp.schema.oaf.Result;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.*;
import org.codehaus.jackson.map.ObjectMapper;
import static org.apache.spark.sql.functions.*;


import java.beans.IntrospectionException;
import java.beans.PropertyDescriptor;
import java.io.IOException;
import java.io.Serializable;
import java.lang.reflect.InvocationTargetException;
import java.util.Arrays;
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
                    .select(col("source"), col("target"));

            Dataset<Row> left = spark.read()
                    .json(tags.getGraphPath() + "/" + tag.getEntityToTag());

            Dataset<Row> right = spark.read()
                    .json(tags.getGraphPath() + "/" + tag.getRelatedEntity().getEntity());


            Dataset<Row> result = left
                    .join(rels, left.col("id").equalTo(rels.col("source")), "left")
                    .join(right, right.col("id").equalTo(rels.col("target")), "left")
                    .select(left.col("*"), right.col(tag.getRelatedEntity().getField())); // selezioni solo quello che ti serve


            Dataset<Row> jsonDataset = result
                            .withColumn("left", to_json(struct(Arrays.stream(result.columns())
                            .filter(c -> !c.equals(tag.getRelatedEntity().getField()))
                            .map(functions::col)
                            .toArray(Column[]::new))))
                    .withColumn("right", to_json(struct(col(tag.getRelatedEntity().getField()))));

            jsonDataset.map((MapFunction<Row, String>)e  -> {
                String leftJson = e.getAs("left");
                String rightJson = e.getAs("right");

                if(rightJson == null)
                    return null;
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

                        ReadContext ctx;
                        ctx = JsonPath.parse(rightJson);
                        // estraggo i valori usando il jsonpath
                        String jsonPath = tag.getRelatedEntity().getJsonPath();
                        if(jsonPath == null)
                            return con.verifyCriteria(value, rightJson);
                        Object comparisonValue = ctx.read(jsonPath);
                        return con.verifyCriteria(value, comparisonValue);


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
