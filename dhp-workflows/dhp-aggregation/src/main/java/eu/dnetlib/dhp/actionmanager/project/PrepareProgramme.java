
package eu.dnetlib.dhp.actionmanager.project;

import static eu.dnetlib.dhp.common.SparkSessionSupport.runWithSparkSession;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.databind.ObjectMapper;

import eu.dnetlib.dhp.actionmanager.project.csvutils.CSVProgramme;
import eu.dnetlib.dhp.application.ArgumentApplicationParser;
import eu.dnetlib.dhp.common.HdfsSupport;
import scala.Tuple2;

public class PrepareProgramme {

	private static final Logger log = LoggerFactory.getLogger(PrepareProgramme.class);
	private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

	public static void main(String[] args) throws Exception {

		String jsonConfiguration = IOUtils
			.toString(
				PrepareProgramme.class
					.getResourceAsStream(
						"/eu/dnetlib/dhp/actionmanager/project/prepare_programme_parameters.json"));

		final ArgumentApplicationParser parser = new ArgumentApplicationParser(jsonConfiguration);

		parser.parseArgument(args);

		Boolean isSparkSessionManaged = Optional
			.ofNullable(parser.get("isSparkSessionManaged"))
			.map(Boolean::valueOf)
			.orElse(Boolean.TRUE);

		log.info("isSparkSessionManaged: {}", isSparkSessionManaged);

		final String programmePath = parser.get("programmePath");
		log.info("programmePath {}: ", programmePath);

		final String outputPath = parser.get("outputPath");
		log.info("outputPath {}: ", outputPath);

		SparkConf conf = new SparkConf();

		runWithSparkSession(
			conf,
			isSparkSessionManaged,
			spark -> {
				removeOutputDir(spark, outputPath);
				exec(spark, programmePath, outputPath);
			});
	}

	private static void removeOutputDir(SparkSession spark, String path) {
		HdfsSupport.remove(path, spark.sparkContext().hadoopConfiguration());
	}

//	private static void exec(SparkSession spark, String programmePath, String outputPath) {
//		Dataset<CSVProgramme> programme = readPath(spark, programmePath, CSVProgramme.class);
//
//		programme
//			.toJavaRDD()
//			.filter(p -> !p.getCode().contains("FP7"))
//			.mapToPair(csvProgramme -> new Tuple2<>(csvProgramme.getCode(), csvProgramme))
//			.reduceByKey((a, b) -> {
//				if (StringUtils.isEmpty(a.getShortTitle())) {
//					if (StringUtils.isEmpty(b.getShortTitle())) {
//						if (StringUtils.isEmpty(a.getTitle())) {
//							if (StringUtils.isNotEmpty(b.getTitle())) {
//								a.setShortTitle(b.getTitle());
//								a.setLanguage(b.getLanguage());
//							}
//						} else {// notIsEmpty a.getTitle
//							if (StringUtils.isEmpty(b.getTitle())) {
//								a.setShortTitle(a.getTitle());
//							} else {
//								if (b.getLanguage().equalsIgnoreCase("en")) {
//									a.setShortTitle(b.getTitle());
//									a.setLanguage(b.getLanguage());
//								} else {
//									a.setShortTitle(a.getTitle());
//								}
//							}
//						}
//					} else {// not isEmpty b.getShortTitle
//						a.setShortTitle(b.getShortTitle());
//						// a.setLanguage(b.getLanguage());
//					}
//				}
//				return a;
//
//			})
//			.map(p -> {
//				CSVProgramme csvProgramme = p._2();
//				if (StringUtils.isEmpty(csvProgramme.getShortTitle())) {
//					csvProgramme.setShortTitle(csvProgramme.getTitle());
//				}
//				return OBJECT_MAPPER.writeValueAsString(csvProgramme);
//			})
//			.saveAsTextFile(outputPath);
//
//	}

	private static void exec(SparkSession spark, String programmePath, String outputPath) {
		Dataset<CSVProgramme> programme = readPath(spark, programmePath, CSVProgramme.class);

		JavaRDD<CSVProgramme> h2020Programmes = programme
			.toJavaRDD()
			.filter(p -> !p.getCode().contains("FP7"))
			.mapToPair(csvProgramme -> new Tuple2<>(csvProgramme.getCode(), csvProgramme))
			.reduceByKey((a, b) -> {
				if (!a.getLanguage().equals("en")) {
					if (b.getLanguage().equalsIgnoreCase("en")) {
						a.setTitle(b.getTitle());
						a.setLanguage(b.getLanguage());
					}
				}
				if (StringUtils.isEmpty(a.getShortTitle())) {
					if (!StringUtils.isEmpty(b.getShortTitle())) {
						a.setShortTitle(b.getShortTitle());
					}
				}

				return a;

			})
			.map(p -> {
				CSVProgramme csvProgramme = p._2();
				String programmeTitle = csvProgramme.getTitle().trim();
				if (programmeTitle.length() > 8 && programmeTitle.substring(0, 8).equalsIgnoreCase("PRIORITY")) {
					programmeTitle = programmeTitle.substring(9);
					if (programmeTitle.charAt(0) == '\'') {
						programmeTitle = programmeTitle.substring(1);
					}
					if (programmeTitle.charAt(programmeTitle.length() - 1) == '\'') {
						programmeTitle = programmeTitle.substring(0, programmeTitle.length() - 1);
					}
					csvProgramme.setTitle(programmeTitle);
				}
				return csvProgramme;
			});

		Object[] codedescription = h2020Programmes
			.map(value -> new Tuple2<>(value.getCode(), value.getTitle()))
			.collect()
			.toArray();

		for (int i = 0; i < codedescription.length - 1; i++) {
			for (int j = i + 1; j < codedescription.length; j++) {
				Tuple2<String, String> t2i = (Tuple2<String, String>) codedescription[i];
				Tuple2<String, String> t2j = (Tuple2<String, String>) codedescription[j];
				if (t2i._1().compareTo(t2j._1()) > 0) {
					Tuple2<String, String> temp = t2i;
					codedescription[i] = t2j;
					codedescription[j] = temp;
				}
			}
		}

		Map<String, String> map = new HashMap<>();
		for (int j = 0; j < codedescription.length; j++) {
			Tuple2<String, String> entry = (Tuple2<String, String>) codedescription[j];
			String ent = entry._1();
			if (ent.contains("Euratom-")) {
				ent = ent.replace("-Euratom-", ".Euratom.");
			}
			String[] tmp = ent.split("\\.");
			if (tmp.length <= 2) {
				map.put(entry._1(), entry._2());

			} else {
				if (ent.endsWith(".")) {
					ent = ent.substring(0, ent.length() - 1);
				}
				String key = ent.substring(0, ent.lastIndexOf(".") + 1);
				if (key.contains("Euratom")) {
					key = key.replace(".Euratom.", "-Euratom-");
					ent = ent.replace(".Euratom.", "-Euratom-");
					if (key.endsWith("-")) {
						key = key.substring(0, key.length() - 1);
					}
				}
				String current = entry._2();
				if (!ent.contains("Euratom")) {

					String parent;
					String tmp_key = tmp[0] + ".";
					for (int i = 1; i < tmp.length - 1; i++) {
						tmp_key += tmp[i] + ".";
						parent = map.get(tmp_key).toLowerCase().trim();
						if (parent.contains("|")) {
							parent = parent.substring(parent.lastIndexOf("|") + 1).trim();
						}
						if (current.trim().length() > parent.length()
							&& current.toLowerCase().trim().substring(0, parent.length()).equals(parent)) {
							current = current.substring(parent.length() + 1);
							if (current.trim().charAt(0) == '-') {
								current = current.trim().substring(1).trim();
							}

						}
					}

				}
				map.put(ent + ".", map.get(key) + " | " + current);
//					String current = entry._2();
//					String parent;
//					String tmp_key = tmp[0] + ".";
//					for (int i = 1; i< tmp.length -1; i++){
//						tmp_key += tmp[i] + ".";
//						parent = map.get(tmp_key).toLowerCase().trim();
//						if (current.trim().length() > parent.length() && current.toLowerCase().trim().substring(0, parent.length()).equals(parent)){
//							current = current.substring(parent.length()+1);
//							if(current.trim().charAt(0) == '-'){
//								current = current.trim().substring(1).trim();
//							}
//
//						}
//					}
//
//					map.put(ent + ".", map.get(key)  + " $ " + current);
			}

		}

		h2020Programmes.map(csvProgramme -> {
			if (!csvProgramme.getCode().endsWith(".") && !csvProgramme.getCode().contains("Euratom")
				&& !csvProgramme.getCode().equals("H2020-EC"))
				csvProgramme.setClassification(map.get(csvProgramme.getCode() + "."));
			else
				csvProgramme.setClassification(map.get(csvProgramme.getCode()));
			return OBJECT_MAPPER.writeValueAsString(csvProgramme);
		}).saveAsTextFile(outputPath);

	}

	public static <R> Dataset<R> readPath(
		SparkSession spark, String inputPath, Class<R> clazz) {
		return spark
			.read()
			.textFile(inputPath)
			.map((MapFunction<String, R>) value -> OBJECT_MAPPER.readValue(value, clazz), Encoders.bean(clazz));
	}

}
