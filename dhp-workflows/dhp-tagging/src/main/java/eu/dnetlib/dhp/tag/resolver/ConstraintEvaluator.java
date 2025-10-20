package eu.dnetlib.dhp.tag.resolver;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.gson.Gson;
import com.jayway.jsonpath.JsonPath;
import com.jayway.jsonpath.ReadContext;
import eu.dnetlib.dhp.api.Utils;
import eu.dnetlib.dhp.application.ArgumentApplicationParser;
import eu.dnetlib.dhp.bulktag.SparkBulkTagJob;
import eu.dnetlib.dhp.bulktag.criteria.VerbResolverFactory;
import org.apache.commons.io.IOUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.function.MapFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.beans.IntrospectionException;
import java.beans.PropertyDescriptor;
import java.io.IOException;
import java.io.Serializable;
import java.lang.reflect.InvocationTargetException;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Objects;
import java.util.Optional;

import static eu.dnetlib.dhp.common.SparkSessionSupport.runWithSparkSession;
import static org.apache.spark.sql.functions.*;

public class ConstraintEvaluator implements Serializable {
    private static final Logger log = LoggerFactory.getLogger(ConstraintEvaluator.class);
    private final static ObjectMapper MAPPER = new ObjectMapper();

    public static void main(String[] args) throws Exception {
        String jsonConfiguration = IOUtils
                .toString(
                        SparkBulkTagJob.class
                                .getResourceAsStream(
                                        "/eu/dnetlib/dhp/wf/subworkflows/bulktag/input_bulkTag_parameters.json"));

        log.info(args.toString());
        final ArgumentApplicationParser parser = new ArgumentApplicationParser(jsonConfiguration);
        parser.parseArgument(args);

        Boolean isSparkSessionManaged = Optional
                .ofNullable(parser.get("isSparkSessionManaged"))
                .map(Boolean::valueOf)
                .orElse(Boolean.TRUE);
        log.info("isSparkSessionManaged: {}", isSparkSessionManaged);

        final String inputPath = parser.get("sourcePath");
        log.info("inputPath: {}", inputPath);

        final String outputPath = parser.get("outputPath");
        log.info("outputPath: {}", outputPath);

        final String baseURL = parser.get("baseURL");
        log.info("baseURL: {}", baseURL);

        log.info("pathMap: {}", parser.get("pathMap"));
        String protoMappingPath = parser.get("pathMap");

        final String hdfsNameNode = parser.get("nameNode");
        log.info("nameNode: {}", hdfsNameNode);

        Configuration configuration = new Configuration();
        configuration.set("fs.defaultFS", hdfsNameNode);
        FileSystem fs = FileSystem.get(configuration);

        String temp = IOUtils.toString(fs.open(new Path(protoMappingPath)), StandardCharsets.UTF_8);
        log.info("protoMap: {}", temp);
        ProtoMap protoMap = new Gson().fromJson(temp, ProtoMap.class);
        log.info("pathMap: {}", new Gson().toJson(protoMap));

        temp = IOUtils.toString(fs.open(new Path("taggingPath")), StandardCharsets.UTF_8);
        log.info("tagging: {}", temp);
        TaggingConstraints taggingConstraints = new Gson()
                .fromJson(temp, TaggingConstraints.class);

        final String dbUrl = parser.get("dbUrl");
        log.info("dbUrl: {}", dbUrl);
        final String dbUser = parser.get("dbUser");
        log.info("dbUser: {}", dbUser);
        final String dbPassword = parser.get("dbPassword");
        log.info("dbPassword: {}", dbPassword);
        final String hdfsPath = outputPath + "masterDuplicate";
        log.info("hdfsPath: {}", hdfsPath);

        //final String configurationPath = parser.get("configurationPath");



        taggingConstraints.getTags().forEach(t -> t.setSelection(VerbResolverFactory.newInstance()));

        SparkConf conf = new SparkConf();

        //todo the community configuration must be included in the tagging
        CommunityConfiguration cc;

        String taggingConf = Optional
                .ofNullable(parser.get("taggingConf"))
                .map(String::valueOf)
                .orElse(null);

        if (taggingConf != null) {
            cc = CommunityConfigurationFactory.newInstance(taggingConf);
        } else {
            cc = Utils.getCommunityConfiguration(baseURL);
            //writeCommunityConfiguration(configurationPath, hdfsNameNode, cc);
        }

        runWithSparkSession(
                conf,
                isSparkSessionManaged,
                spark -> {

                    execTagging(
                            spark, inputPath, outputPath, protoMap, taggingConstraints);


                });
    }

    private static void execTagging(SparkSession spark, String inputPath, String outputPath, ProtoMap protoMap, TaggingConstraints tags) {
        tags.getTags().forEach(t -> {
            try {
                evaluate(spark, t, inputPath, protoMap);
            } catch (ClassNotFoundException e) {
                throw new RuntimeException(e);
            }
        });
    }


    private static  void evaluate(SparkSession spark, TaggingConstraint tag, String inputPath, ProtoMap protoMap) throws ClassNotFoundException {
        if(tag.getRelatedEntity() != null){

            Dataset<Row> linking_entity = spark.read()
                    .json(inputPath + tag.getRelatedEntity().getLinkingResource())
                    .where(tag.getRelatedEntity().getLinkingAttribute() + " = '" + tag.getRelatedEntity().getLinkingAttributeValue() + "'")
                    .select(tag.getRelatedEntity().getAttributes_to_select().stream().map(functions::col).toArray(Column[]::new));

            Dataset<Row> left = spark.read()
                    .json(inputPath + tag.getEntityToTag());

            Dataset<Row> right = spark.read()
                    .json(inputPath + tag.getRelatedEntity().getEntity());


            Dataset<Row> result = left
                    .join(linking_entity, left.col(tag.getJoinOn()).equalTo(linking_entity.col(tag.getRelatedEntity().getJoinOnLeft())))
                    .join(right, right.col(tag.getJoinOn()).equalTo(linking_entity.col(tag.getRelatedEntity().getJoinOnRigth())))
                    .select(left.col("*"), right.col(tag.getRelatedEntity().getLinkedResourceField()));


            Dataset<Row> jsonDataset = result
                            .withColumn("left", to_json(struct(Arrays.stream(result.columns())
                            .filter(c -> !c.equals(tag.getRelatedEntity().getLinkedResourceField()))
                            .map(functions::col)
                            .toArray(Column[]::new))))
                    .withColumn("right", to_json(struct(col(tag.getRelatedEntity().getLinkedResourceField()))));

            jsonDataset.map((MapFunction<Row, String>)e  -> {
                String leftJson = e.getAs("left");
                String rightJson = e.getAs("right");

                if(rightJson == null)
                    return null;
                if(Boolean.TRUE.equals(verifyCriteria(tag, leftJson, rightJson, protoMap))){
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

        }else{

        }

    }

    private static Boolean verifyCriteria(TaggingConstraint tag,
                                          String leftJson,
                                          String rightJson,
                                          ProtoMap protoMap){
        tag.getCriteria().stream().anyMatch(c -> c.getConstraint().stream().allMatch(con -> {
            Object value = null;
            ReadContext context = JsonPath.parse(leftJson);
            if (protoMap.containsKey(con.getField())){
                value = context.read(protoMap.get(con.getField()).getPath());
            } else {
                value = context.read(con.getField());
            }
            context = JsonPath.parse(rightJson);
            String jsonPath = tag.getRelatedEntity().getJsonPath();
            if(jsonPath == null)
                    return con.verifyCriteria(value, rightJson);
            Object comparisonValue = context.read(jsonPath);
            return con.verifyCriteria(value, comparisonValue);

//            ObjectMapper mapper = new ObjectMapper();
//            String className = tag.getEntityClass();
//            Class<?> clazz = null;
//            try {
//                clazz = Class.forName(className);
//            } catch (ClassNotFoundException ex) {
//                throw new RuntimeException(ex);
//            }
//            try {
//                Object leftInstance = mapper.readValue(leftJson, clazz);
//                String fieldName =  con.getField();//lo prendo dalla path map cosi' evito di dover usare la reflection
//                PropertyDescriptor pd = new PropertyDescriptor(fieldName, leftInstance.getClass());
//                Object value = pd.getReadMethod().invoke(leftInstance);
//
//                ReadContext ctx;
//                ctx = JsonPath.parse(rightJson);
//                // estraggo i valori usando il jsonpath
//                String jsonPath = tag.getRelatedEntity().getJsonPath();
//                if(jsonPath == null)
//                    return con.verifyCriteria(value, rightJson);
//                Object comparisonValue = ctx.read(jsonPath);
//                return con.verifyCriteria(value, comparisonValue);
//
//
//            } catch (IOException ex) {
//                throw new RuntimeException(ex);
//            } catch (IntrospectionException ex) {
//                throw new RuntimeException(ex);
//            } catch (InvocationTargetException ex) {
//                throw new RuntimeException(ex);
//            } catch (IllegalAccessException ex) {
//                throw new RuntimeException(ex);
//            }

        }));
        return null;
    }

}
