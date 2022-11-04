
package eu.dnetlib.dhp.subjecttoresultfromsemrel;

import static eu.dnetlib.dhp.PropagationConstant.*;
import static eu.dnetlib.dhp.common.SparkSessionSupport.runWithSparkSession;

import java.io.Serializable;
import java.util.*;
import java.util.stream.Collectors;

import org.apache.commons.io.IOUtils;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import eu.dnetlib.dhp.application.ArgumentApplicationParser;
import eu.dnetlib.dhp.schema.common.ModelConstants;
import eu.dnetlib.dhp.schema.oaf.Result;
import eu.dnetlib.dhp.schema.oaf.Subject;
import eu.dnetlib.dhp.schema.oaf.utils.OafMapperUtils;
import scala.Tuple2;

/**
 * @author miriam.baglioni
 * @Date 05/10/22
 */
public class SparkSubjectPropagationStep2 implements Serializable {
	private static final Logger log = LoggerFactory.getLogger(SparkSubjectPropagationStep2.class);

	public static void main(String[] args) throws Exception {
		String jsonConfiguration = IOUtils
			.toString(
				SparkSubjectPropagationStep2.class
					.getResourceAsStream(
						"/eu/dnetlib/dhp/subjectpropagation/input_propagatesubject_parameters.json"));

		final ArgumentApplicationParser parser = new ArgumentApplicationParser(jsonConfiguration);
		parser.parseArgument(args);

		Boolean isSparkSessionManaged = isSparkSessionManaged(parser);
		log.info("isSparkSessionManaged: {}", isSparkSessionManaged);

		String preparedPath = parser.get("preparedPath");
		log.info("preparedPath: {}", preparedPath);

		final String outputPath = parser.get("outputPath");
		log.info("outputPath: {}", outputPath);

		final String resultClassName = parser.get("resultTableName");
		log.info("resultTableName: {}", resultClassName);

		Class<? extends Result> resultClazz = (Class<? extends Result>) Class.forName(resultClassName);

		final String resultType = parser.get("resultType");
		log.info("resultType: {}", resultType);

		final String inputPath = parser.get("sourcePath");
		log.info("inputPath: {}", inputPath);

		final String workingPath = parser.get("workingPath");
		log.info("workingPath: {}", workingPath);

		SparkConf conf = new SparkConf();
		conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer");
		conf.registerKryoClasses(getModelClasses());
		runWithSparkSession(
			conf,
			isSparkSessionManaged,
			spark -> {
				removeOutputDir(spark, outputPath);
				execPropagation(spark, inputPath, outputPath, workingPath, preparedPath, resultClazz, resultType);
			});
	}

	private static <R extends Result> void execPropagation(SparkSession spark,
		String inputPath,
		String outputPath,
		String workingPath,
		String preparedPath,
		Class<R> resultClazz,
		String resultType) {

		Dataset<Tuple2<String, R>> results = readOafKryoPath(spark, inputPath + "/" + resultType, resultClazz)
			.map(
				(MapFunction<R, Tuple2<String, R>>) r -> new Tuple2(r.getId(), r),
				Encoders.tuple(Encoders.STRING(), Encoders.kryo(resultClazz)));

		Dataset<ResultSubjectList> preparedResult = readPath(
			spark, preparedPath + "/publication", ResultSubjectList.class)
				.union(readPath(spark, preparedPath + "/dataset", ResultSubjectList.class))
				.union(readPath(spark, preparedPath + "/software", ResultSubjectList.class))
				.union(readPath(spark, preparedPath + "/otherresearchproduct", ResultSubjectList.class));

		results
			.joinWith(
				preparedResult,
				results.col("_1").equalTo(preparedResult.col("resId")),
				"left")
			.map((MapFunction<Tuple2<Tuple2<String, R>, ResultSubjectList>, String>) t2 -> {
				R res = t2._1()._2();
				// estraggo le tipologie di subject dal result
				Map<String, List<String>> resultMap = new HashMap<>();
				if (Optional.ofNullable(t2._2()).isPresent()) {
					if(Optional.ofNullable(res.getSubject()).isPresent()){
						res.getSubject().stream().forEach(s -> {
							String cid = s.getQualifier().getClassid();
							if(!cid.equals(ModelConstants.DNET_SUBJECT_KEYWORD)){
								if (!resultMap.containsKey(cid)) {
									resultMap.put(cid, new ArrayList<>());
								}
								resultMap.get(cid).add(s.getValue());
							}
						});
					}else{
						res.setSubject(new ArrayList<>());
					}

					// Remove from the list all the subjects with the same class already present in the result
					List<String> distinctClassId = t2
						._2()
						.getSubjectList()
						.stream()
						.map(si -> si.getClassid())
						.distinct()
						.collect(Collectors.toList());
					List<SubjectInfo> sbjInfo = new ArrayList<>();
					for (String k : distinctClassId) {
						if (!resultMap.containsKey(k))
							sbjInfo = t2
								._2()
								.getSubjectList()
								.stream()
								.filter(s -> s.getClassid().equalsIgnoreCase(k))
								.collect(Collectors.toList());
						else
							sbjInfo = t2
								._2()
								.getSubjectList()
								.stream()
								.filter(
									s -> s.getClassid().equalsIgnoreCase(k) &&
										!resultMap.get(k).contains(s.getValue()))
								.collect(Collectors.toList());
						// All the subjects not already present in the result are added
						for (SubjectInfo si : sbjInfo) {
							res.getSubject().add(getSubject(si));
						}

					}

				}
				return OBJECT_MAPPER.writeValueAsString(res);
			}, Encoders.STRING())
			.write()
			.mode(SaveMode.Overwrite)
			.option("compression", "gzip")
			.text(workingPath + "/" + resultType);

		readPath(spark, workingPath + "/" + resultType, resultClazz)
			.write()
			.mode(SaveMode.Overwrite)
			.option("compression", "gzip")
			.json(outputPath + "/" + resultType);

	}

	private static <R extends Result> Subject getSubject(SubjectInfo si) {
		return OafMapperUtils
			.subject(
				si.getValue(),
				si.getClassid(), si.getClassname(),
				ModelConstants.DNET_SUBJECT_TYPOLOGIES, ModelConstants.DNET_SUBJECT_TYPOLOGIES,
				OafMapperUtils
					.dataInfo(
						false, PROPAGATION_DATA_INFO_TYPE,
						true, false,
						OafMapperUtils
							.qualifier(
								PROPAGATION_SUBJECT_RESULT_SEMREL_CLASS_ID,
								PROPAGATION_SUBJECT_RESULT_SEMREL_CLASS_NAME,
								ModelConstants.DNET_PROVENANCE_ACTIONS,
								ModelConstants.DNET_PROVENANCE_ACTIONS),
						"0.85"));

	}

}
