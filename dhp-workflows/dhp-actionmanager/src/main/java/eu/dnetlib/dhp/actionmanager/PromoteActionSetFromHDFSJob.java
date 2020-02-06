package eu.dnetlib.dhp.actionmanager;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import eu.dnetlib.dhp.application.ArgumentApplicationParser;
import eu.dnetlib.dhp.schema.oaf.OafEntity;
import eu.dnetlib.dhp.schema.oaf.Software;
import org.apache.commons.io.IOUtils;
import org.apache.hadoop.io.Text;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.api.java.function.ReduceFunction;
import org.apache.spark.sql.*;
import org.apache.spark.sql.types.*;
import scala.Tuple2;

import java.io.IOException;
import java.util.Collections;

import static org.apache.spark.sql.functions.*;

public class PromoteActionSetFromHDFSJob {

    public static void main(String[] args) throws Exception {
        final ArgumentApplicationParser parser = new ArgumentApplicationParser(IOUtils.toString(
                PromoteActionSetFromHDFSJob.class
                        .getResourceAsStream("/eu/dnetlib/dhp/actionmanager/actionmanager_input_parameters.json")));
        parser.parseArgument(args);
        String inputActionSetPath = parser.get("input");
        String outputPath = parser.get("output");

        final SparkConf conf = new SparkConf();
        conf.setMaster(parser.get("master"));
        conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer");

        try (SparkSession spark = SparkSession.builder().config(conf).getOrCreate()) {
            // reading actions as RDD
            JavaRDD<Row> actionsRDD = JavaSparkContext
                    .fromSparkContext(spark.sparkContext())
                    .sequenceFile(inputActionSetPath, Text.class, Text.class)
                    .map(x -> RowFactory.create(x._2().toString()));

            // converting actions to DataFrame and deserializing content of TargetValue
            // using unbase64 on TargetValue content to get String representation
            StructType rowSchema = StructType$.MODULE$.apply(
                    Collections.singletonList(
                            StructField$.MODULE$.apply("value", DataTypes.StringType, false, Metadata.empty())
                    ));
            Dataset<Row> deserializedTargetValue = spark.createDataFrame(actionsRDD, rowSchema)
                    .withColumn("TargetValue", get_json_object(col("value"), "$.TargetValue"))
                    .select(unbase64(col("TargetValue")).cast(DataTypes.StringType).as("target_value_json"))
                    .cache();

            // printing: only for testing
            deserializedTargetValue.printSchema();
            deserializedTargetValue.show();
            System.out.println(deserializedTargetValue.first().toString());

            // grouping and merging: should be generic
            Dataset<Software> softwareDS = deserializedTargetValue
                    .map((MapFunction<Row, Software>) PromoteActionSetFromHDFSJob::rowToOafEntity, Encoders.kryo(Software.class))
                    .groupByKey((MapFunction<Software, String>) OafEntity::getId, Encoders.STRING())
                    .reduceGroups((ReduceFunction<Software>) (software1, software2) -> {
                        software1.mergeFrom(software2);
                        return software1;
                    })
                    .map((MapFunction<Tuple2<String, Software>, Software>) pair -> pair._2, Encoders.kryo(Software.class));

            softwareDS.printSchema();
            softwareDS.show();

            // save
//            softwareDS.toDF()
//                    .write()
//                    .partitionBy("id")
//                    .save(outputPath);

            // another approach: using only DataFrames i.e. DataSet<Row>, not DataSets<Software>
        }
    }

    private static Software rowToOafEntity(Row row) {
        // converts row with JSON into Software object: should be generic
        // currently extracts only "entity.id" field from JSON
        ObjectMapper objectMapper = new ObjectMapper();
        try {
            JsonNode jsonNode = objectMapper.readTree(row.getString(0));
            String id = jsonNode.at("/entity/id").asText();
            Software software = new Software();
            software.setId(id);
            return software;
        } catch (IOException e) {
            e.printStackTrace();
            throw new RuntimeException(e);
        }
    }

}
