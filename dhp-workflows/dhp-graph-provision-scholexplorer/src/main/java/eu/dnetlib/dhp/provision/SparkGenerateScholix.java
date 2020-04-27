
package eu.dnetlib.dhp.provision;

import com.fasterxml.jackson.databind.ObjectMapper;
import eu.dnetlib.dhp.application.ArgumentApplicationParser;
import eu.dnetlib.dhp.provision.scholix.*;
import eu.dnetlib.dhp.provision.scholix.summary.ScholixSummary;
import eu.dnetlib.dhp.schema.oaf.Relation;
import org.apache.commons.io.IOUtils;
import org.apache.hadoop.io.compress.GzipCodec;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;
import scala.Tuple2;

public class SparkGenerateScholix {

	public static void main(String[] args) throws Exception {
		final ArgumentApplicationParser parser = new ArgumentApplicationParser(
			IOUtils
				.toString(
					SparkGenerateScholix.class
						.getResourceAsStream(
							"/eu/dnetlib/dhp/provision/input_generate_summary_parameters.json")));
		parser.parseArgument(args);
		SparkConf conf = new SparkConf();
		conf.set("spark.sql.shuffle.partitions", "4000");
		conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer");
		final SparkSession spark = SparkSession
			.builder()
			.config(conf)
			.appName(SparkExtractRelationCount.class.getSimpleName())
			.master(parser.get("master"))
			.getOrCreate();

		conf
			.registerKryoClasses(
				new Class[] {
					Scholix.class, ScholixCollectedFrom.class, ScholixEntityId.class,
					ScholixIdentifier.class, ScholixRelationship.class, ScholixResource.class
				});

		final String graphPath = parser.get("graphPath");
		final String workingDirPath = parser.get("workingDirPath");

		final JavaSparkContext sc = new JavaSparkContext(spark.sparkContext());

		final Dataset<ScholixSummary> scholixSummary = spark
			.read()
			.load(workingDirPath + "/summary")
			.as(Encoders.bean(ScholixSummary.class));
		final Dataset<Relation> rels = spark.read().load(graphPath + "/relation").as(Encoders.bean(Relation.class));

		Dataset<Scholix> firstJoin = scholixSummary
			.joinWith(rels, scholixSummary.col("id").equalTo(rels.col("source")))
			.map(
				(MapFunction<Tuple2<ScholixSummary, Relation>, Scholix>) f -> Scholix
					.generateScholixWithSource(f._1(), f._2()),
				Encoders.bean(Scholix.class));

		firstJoin.write().mode(SaveMode.Overwrite).save(workingDirPath + "/scholix_1");

		Dataset<Scholix> scholix_final = spark
			.read()
			.load(workingDirPath + "/scholix_1")
			.as(Encoders.bean(Scholix.class));

		scholixSummary
			.map(
				(MapFunction<ScholixSummary, ScholixResource>) ScholixResource::fromSummary,
				Encoders.bean(ScholixResource.class))
			.repartition(1000)
			.write()
			.mode(SaveMode.Overwrite)
			.save(workingDirPath + "/scholix_target");

		Dataset<ScholixResource> target = spark
			.read()
			.load(workingDirPath + "/scholix_target")
			.as(Encoders.bean(ScholixResource.class));

		scholix_final
			.joinWith(
				target, scholix_final.col("identifier").equalTo(target.col("dnetIdentifier")), "inner")
			.map(
				(MapFunction<Tuple2<Scholix, ScholixResource>, Scholix>) f -> {
					final Scholix scholix = f._1();
					final ScholixResource scholixTarget = f._2();
					scholix.setTarget(scholixTarget);
					scholix.generateIdentifier();
					scholix.generatelinkPublisher();
					return scholix;
				},
				Encoders.kryo(Scholix.class))
			.javaRDD()
			.map(
				s -> {
					ObjectMapper mapper = new ObjectMapper();
					return mapper.writeValueAsString(s);
				})
			.saveAsTextFile(workingDirPath + "/scholix_json", GzipCodec.class);
	}
}
