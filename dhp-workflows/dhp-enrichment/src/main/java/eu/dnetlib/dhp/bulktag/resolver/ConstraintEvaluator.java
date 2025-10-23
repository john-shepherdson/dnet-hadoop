package eu.dnetlib.dhp.bulktag.resolver;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.gson.Gson;
import com.jayway.jsonpath.JsonPath;
import com.jayway.jsonpath.PathNotFoundException;
import com.jayway.jsonpath.ReadContext;
import eu.dnetlib.dhp.application.ArgumentApplicationParser;
import eu.dnetlib.dhp.bulktag.SparkBulkTagJob;
import eu.dnetlib.dhp.bulktag.community.*;
import eu.dnetlib.dhp.bulktag.criteria.VerbResolverFactory;
import org.apache.commons.io.IOUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Tuple2;

import static eu.dnetlib.dhp.common.SparkSessionSupport.runWithSparkSession;


import java.io.Serializable;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.stream.Collectors;

public class ConstraintEvaluator implements Serializable {
    private static final Logger log = LoggerFactory.getLogger(ConstraintEvaluator.class);
    private final static ObjectMapper MAPPER = new ObjectMapper();

    public static void main(String[] args) throws Exception {
        String jsonConfiguration = IOUtils
                .toString(
                        SparkBulkTagJob.class
                                .getResourceAsStream(
                                        "/eu/dnetlib/dhp/wf/subworkflows/bulktag/input_tagging_parameters.json"));

        log.info(args.toString());
        final ArgumentApplicationParser parser = new ArgumentApplicationParser(jsonConfiguration);
        parser.parseArgument(args);

        Boolean isSparkSessionManaged = Optional
                .ofNullable(parser.get("isSparkSessionManaged"))
                .map(Boolean::valueOf)
                .orElse(Boolean.TRUE);
        log.info("isSparkSessionManaged: {}", isSparkSessionManaged);

        final String outputPath = parser.get("outputPath");
        log.info("outputPath: {}", outputPath);

        log.info("pathMapPath: {}", parser.get("pathMapPath"));
        String protoMappingPath = parser.get("pathMapPath");

        final String hdfsNameNode = parser.get("nameNode");
        log.info("nameNode: {}", hdfsNameNode);

        Configuration configuration = new Configuration();
        configuration.set("fs.defaultFS", hdfsNameNode);
        FileSystem fs = FileSystem.get(configuration);

        String temp = IOUtils.toString(fs.open(new Path(protoMappingPath)), StandardCharsets.UTF_8);
        log.info("protoMap: {}", temp);
        ProtoMap protoMap = new Gson().fromJson(temp, ProtoMap.class);
        log.info("pathMap: {}", new Gson().toJson(protoMap));

        temp = IOUtils.toString(fs.open(new Path(parser.get("taggingPath"))), StandardCharsets.UTF_8);
        log.info("tagging: {}", temp);
        TaggingConstraints taggingConstraints = new Gson()
                .fromJson(temp, TaggingConstraints.class);



        taggingConstraints.getTags().forEach(t -> t.setSelection(VerbResolverFactory.newInstance()));

        SparkConf conf = new SparkConf();


        runWithSparkSession(
                conf,
                isSparkSessionManaged,
                spark -> {

                    execTagging(
                            spark, outputPath, protoMap, taggingConstraints);


                });
    }

    private static void execTagging(SparkSession spark, String outputPath, ProtoMap protoMap, TaggingConstraints tags) {
        tags.getTags().forEach(t -> {
            try {
                evaluate(spark, t, outputPath, protoMap);
            } catch (ClassNotFoundException e) {
                throw new RuntimeException(e);
            }
        });
    }


//    private static  void evaluate(SparkSession spark, TaggingConstraint tag, String inputPath, ProtoMap protoMap) throws ClassNotFoundException {
//        if(tag.getRelatedEntity() != null){
//
//            Dataset<Row> linking_entity = spark.read()
//                    .json(inputPath + tag.getRelatedEntity().getLinkingResource())
//                    .where(tag.getRelatedEntity().getLinkingAttribute() + " = '" + tag.getRelatedEntity().getLinkingAttributeValue() + "'")
//                    .select(tag.getRelatedEntity().getAttributes_to_select().stream().map(functions::col).toArray(Column[]::new));
//
//            Dataset<Row> left = spark.read()
//                    .json(inputPath + tag.getEntityToTag());
//
//            Dataset<Row> right = spark.read()
//                    .json(inputPath + tag.getRelatedEntity().getEntity());
//
//
//            Dataset<Row> result = left
//                    .join(linking_entity, left.col(tag.getJoinOn()).equalTo(linking_entity.col(tag.getRelatedEntity().getJoinOnLeft())))
//                    .join(right, right.col(tag.getJoinOn()).equalTo(linking_entity.col(tag.getRelatedEntity().getJoinOnRigth())))
//                    .select(left.col("*"), right.col(tag.getRelatedEntity().getLinkedResourceField()));
//
//
//            Dataset<Row> jsonDataset = result
//                            .withColumn("left", to_json(struct(Arrays.stream(result.columns())
//                            .filter(c -> !c.equals(tag.getRelatedEntity().getLinkedResourceField()))
//                            .map(functions::col)
//                            .toArray(Column[]::new))))
//                    .withColumn("right", to_json(struct(col(tag.getRelatedEntity().getLinkedResourceField()))));
//
//            jsonDataset.map((MapFunction<Row, String>)e  -> {
//                String leftJson = e.getAs("left");
//                String rightJson = e.getAs("right");
//
//                if(rightJson == null)
//                    return null;
//                if(Boolean.TRUE.equals(verifyCriteria(tag, leftJson, rightJson, protoMap))){
//                    ObjectMapper mapper = new ObjectMapper();
//                    String className = tag.getEntityClass();
//                    Class<?> clazz = null;
//                    try {
//                        clazz = Class.forName(className);
//                    } catch (ClassNotFoundException ex) {
//                        throw new RuntimeException(ex);
//                    }
//                    try {
//                        Object leftInstance = mapper.readValue(leftJson, clazz);
//                        PropertyDescriptor pd = new PropertyDescriptor("id", leftInstance.getClass());
//                        return pd.getReadMethod().invoke(leftInstance) + "@@" + tag.getId();
//                    } catch (IOException ex) {
//                        throw new RuntimeException(ex);
//                    } catch (IntrospectionException ex) {
//                        throw new RuntimeException(ex);
//                    } catch (InvocationTargetException ex) {
//                        throw new RuntimeException(ex);
//                    } catch (IllegalAccessException ex) {
//                        throw new RuntimeException(ex);
//                    }
//                }
//
//                return null;
//            }, Encoders.STRING())
//                    .filter(Objects::nonNull);
//
//        }else{
//
//        }
//
//    }

    private static  void evaluate(SparkSession spark, TaggingConstraint tag, String outputPath, ProtoMap protoMap) throws ClassNotFoundException {
        for (String tablename : tag.getInputs().keySet()) {
            spark.read().json(tag.getInputs().get(tablename))
                    .createOrReplaceTempView(tablename);
        }

        if(tag.getSelects() != null) {
            for (SelectPair sel : tag.getSelects()) {
                spark.sql(sel.getTableSelect()).createOrReplaceTempView(sel.getTableName());
            }
        }

        Dataset<Row> tmp = spark.sql("SELECT * FROM " + tag.getResultTable());
        String structCols ;
        String sql ;
        if (tag.getSelects() != null) {
            structCols= Arrays.stream(tmp.columns())
                    .filter(c -> !c.equals("comparisonValue")) // escludi right
                    .collect(Collectors.joining(", "));
            sql = "SELECT *, " +
                    "to_json(struct(" + structCols + ")) AS left, " +
                    "to_json(struct(comparisonValue)) AS right " +
                    "FROM " + tag.getResultTable();
        }

        else {
            structCols = Arrays.stream(tmp.columns())
                    .collect(Collectors.joining(", "));
            sql = "SELECT *, " +
                    "to_json(struct(" + structCols + ")) AS left, " +
                    "NULL as right " +
                    "FROM " + tag.getResultTable();
        }

        spark.sql(sql).map((MapFunction<Row, Tuple2<String, String>>) r -> {
            String leftJson = r.getAs("left");
            String rightJson = (String) r.getAs("right");

            if (Boolean.TRUE.equals(verifyCriteria(leftJson, rightJson, protoMap, tag.getCriteria()))) {
                ReadContext context = JsonPath.parse(leftJson);
                return new Tuple2<>(context.read(protoMap.get("id").getPath()), tag.getId());
            }
            return null;
        }, Encoders.tuple(Encoders.STRING(), Encoders.STRING()))
                .filter(Objects::nonNull)
                .write()
                .mode(SaveMode.Append)
                .option("compression","gzip")
                .json(outputPath);

    }

        private static Boolean verifyCriteria(
                                          String leftJson,
                                          String rightJson,
                                          ProtoMap protoMap,
                                          List<Constraints> criteria){
        return criteria.stream().anyMatch(c -> c.getConstraint().stream().allMatch(con -> {
            Object value = null;
            //nella configurazione c'e' l'indicazione dell'id nella path map che continene
            //il jsonpath da usare
            String mapKey = con.getJsonPath();
            if (mapKey != null ) {
                if(protoMap.containsKey(mapKey)) {
                    con.setVerbJsonPath(protoMap.get(mapKey).getPath());
                }
            }
            ReadContext context = JsonPath.parse(leftJson);
            if (protoMap.containsKey(con.getField())){
                try {
                    value = context.read(protoMap.get(con.getField()).getPath());

                }catch (PathNotFoundException e) {
                    value = null;
                }
                Object comparisonValue = null;
                if (rightJson != null) {
                    try {
                        comparisonValue = JsonPath.parse(rightJson).read("comparisonValue");
                    } catch (Exception e) {
                        log.warn("Could not read comparisonValue from rightJson: {}", rightJson, e);
                    }
                }
                if(comparisonValue == null)
                    return con.verifyCriteria(value);
                return con.verifyCriteria(value, comparisonValue);

            } else {
                throw new RuntimeException("No path to access for this field. Extend the path Map");
            }

        }));

    }

}
