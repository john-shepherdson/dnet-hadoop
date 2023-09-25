
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
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.databind.ObjectMapper;

import eu.dnetlib.dhp.actionmanager.project.utils.model.CSVProgramme;
import eu.dnetlib.dhp.application.ArgumentApplicationParser;
import eu.dnetlib.dhp.common.HdfsSupport;
import scala.Tuple2;

/**
 * Among all the programmes provided in the csv file, selects those in H2020 framework that have an english title.
 *
 * The title is then handled to get the programme description at a certain level. The set of programme titles will then
 * be used to associate a classification for the programme.
 *
 * The programme code describes an hierarchy that can be exploited to provide the classification. To determine the hierarchy
 * the code can be split by '.'. If the length of the splitted code is less than or equal to 2 it can be directly used
 * as the classification: H2020-EU -> Horizon 2020 Framework Programme (It will never be repeated),
 * H2020-EU.1. -> Excellent science, H2020-EU.2. -> Industrial leadership etc.
 *
 * The codes are ordered and for all of them the concatenation of all the titles (from the element in position 1 of
 * the splitted code) handled as below is used to create the classification. For example:
 *
 *   H2020-EU.1.1       -> Excellent science | European Research Council (ERC)
 *   from H2020-EU.1. -> Excellence science and H2020-EU.1.1. -> European Research Council (ERC)
 *
 *   H2020-EU.3.1.3.1. -> Societal challenges | Health, demographic change and well-being | Treating and managing disease | Treating disease, including developing regenerative medicine
 *   from H2020-EU.3.       -> Societal challenges,
 *        H2020-EU.3.1.     -> Health, demographic change and well-being
 *        H2020-EU.3.1.3    -> Treating and managing disease
 *        H2020-EU.3.1.3.1. -> Treating disease, including developing regenerative medicine
 *
 * The classification up to level three, will be split in dedicated variables, while the complete classification will be stored
 * in a variable called classification and provided as shown above.
 *
 * The programme title is not give in a standardized way:
 *
 *  - Sometimes associated to the higher level in the hierarchy we can find Priority in title other times it is not the
 *    case. Since it is not uniform, we removed priority from the handled titles:
 *
 *    H2020-EU.1. -> PRIORITY 'Excellent science'
 *    H2020-EU.2. -> PRIORITY 'Industrial leadership'
 *    H2020-EU.3. -> PRIORITY 'Societal challenges
 *
 *    will become
 *
 *    H2020-EU.1. -> Excellent science
 *    H2020-EU.2. -> Industrial leadership
 *    H2020-EU.3. -> Societal challenges
 *
 *  - Sometimes the title of the parent is repeated in the title for the code, but it is not always the case, so, titles
 *    associated to previous levels in the hierarchy are removed from the code title.
 *
 *	  H2020-EU.1.2. -> EXCELLENT SCIENCE - Future and Emerging Technologies (FET)
 *	  H2020-EU.2.2. -> INDUSTRIAL LEADERSHIP - Access to risk finance
 *    H2020-EU.3.4. -> SOCIETAL CHALLENGES - Smart, Green And Integrated Transport
 *
 *    will become
 *
 *    H2020-EU.1.2. -> Future and Emerging Technologies (FET)
 *    H2020-EU.2.2. -> Access to risk finance
 *    H2020-EU.3.4. -> Smart, Green And Integrated Transport
 *
 *    This holds at all levels in the hierarchy. Hence
 *
 *    H2020-EU.2.1.2. -> INDUSTRIAL LEADERSHIP - Leadership in enabling and industrial technologies – Nanotechnologies
 *
 *    will become
 *
 *    H2020-EU.2.1.2. -> Nanotechnologies
 *
 *  - Euratom is not given in the way the other programmes are: H2020-EU. but H2020-Euratom- . So we need to write
 *    specific code for it
 *
 *
 *
 */
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

	private static void exec(SparkSession spark, String programmePath, String outputPath) {
		Dataset<CSVProgramme> programme = readPath(spark, programmePath, CSVProgramme.class);

		JavaRDD<CSVProgramme> h2020Programmes = programme
			.toJavaRDD()
			.mapToPair(csvProgramme -> new Tuple2<>(csvProgramme.getCode(), csvProgramme))
			.reduceByKey(PrepareProgramme::groupProgrammeByCode)
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

		final JavaSparkContext jsc = JavaSparkContext.fromSparkContext(spark.sparkContext());

		JavaRDD<CSVProgramme> rdd = jsc.parallelize(prepareClassification(h2020Programmes), 1);
		rdd
			.map(OBJECT_MAPPER::writeValueAsString)
			.saveAsTextFile(outputPath);

	}

	private static CSVProgramme groupProgrammeByCode(CSVProgramme a, CSVProgramme b) {
		if (!a.getLanguage().equals("en") && b.getLanguage().equalsIgnoreCase("en")) {
			a.setTitle(b.getTitle());
			a.setLanguage(b.getLanguage());
		}
		if (StringUtils.isEmpty(a.getShortTitle()) && !StringUtils.isEmpty(b.getShortTitle())) {
			a.setShortTitle(b.getShortTitle());
		}

		return a;
	}

	@SuppressWarnings("unchecked")
	private static List<CSVProgramme> prepareClassification(JavaRDD<CSVProgramme> h2020Programmes) {
		Object[] codedescription = h2020Programmes
			.map(
				value -> new Tuple2<>(value.getCode(),
					new Tuple2<>(value.getTitle(), value.getShortTitle())))
			.collect()
			.toArray();

		for (int i = 0; i < codedescription.length - 1; i++) {
			for (int j = i + 1; j < codedescription.length; j++) {
				Tuple2<String, Tuple2<String, String>> t2i = (Tuple2<String, Tuple2<String, String>>) codedescription[i];
				Tuple2<String, Tuple2<String, String>> t2j = (Tuple2<String, Tuple2<String, String>>) codedescription[j];
				if (t2i._1().compareTo(t2j._1()) > 0) {
					Tuple2<String, Tuple2<String, String>> temp = t2i;
					codedescription[i] = t2j;
					codedescription[j] = temp;
				}
			}
		}

		Map<String, Tuple2<String, String>> map = new HashMap<>();
		for (int j = 0; j < codedescription.length; j++) {
			Tuple2<String, Tuple2<String, String>> entry = (Tuple2<String, Tuple2<String, String>>) codedescription[j];
			String ent = entry._1();
			if (ent.contains("Euratom-")) {
				ent = ent.replace("-Euratom-", ".Euratom.");
			}
			String[] tmp = ent.split("\\.");
			if (tmp.length <= 2) {
				if (StringUtils.isEmpty(entry._2()._2())) {
					map.put(entry._1(), new Tuple2<>(entry._2()._1(), entry._2()._1()));
				} else {
					map.put(entry._1(), entry._2());
				}
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
				String current = entry._2()._1();
				if (!ent.contains("Euratom")) {

					String parent;
					String tmpKey = tmp[0] + ".";
					for (int i = 1; i < tmp.length - 1; i++) {
						tmpKey += tmp[i] + ".";
						parent = map.get(tmpKey)._1().toLowerCase().trim();
						if (parent.contains("|")) {
							parent = parent.substring(parent.lastIndexOf("|") + 1).trim();
						}
						if (current.trim().length() > parent.length()
							&& current.toLowerCase().trim().startsWith(parent)) {
							current = current.substring(parent.length() + 1);
							if (current.trim().charAt(0) == '-' || current.trim().charAt(0) == '–') {
								current = current.trim().substring(1).trim();
							}

						}
					}

				}
				String shortTitle = entry._2()._2();
				if (StringUtils.isEmpty(shortTitle)) {
					shortTitle = current;
				}
				Tuple2<String, String> newEntry = new Tuple2<>(map.get(key)._1() + " | " + current,
					map.get(key)._2() + " | " + shortTitle);
				map.put(ent + ".", newEntry);

			}

		}
		return h2020Programmes.map(csvProgramme -> {

			String code = csvProgramme.getCode();
			if (!code.endsWith(".") && !code.contains("Euratom")
				&& !code.equals("H2020-EC") && !code.equals("H2020") &&
				!code.equals("H2020-Topics"))
				code += ".";

			if (map.containsKey(code)) {
				csvProgramme.setClassification(map.get(code)._1());
				csvProgramme.setClassification_short(map.get(code)._2());
			} else
				log.info("WARNING: No entry in map for code " + code);

			return csvProgramme;
		}).collect();
	}

	public static <R> Dataset<R> readPath(
		SparkSession spark, String inputPath, Class<R> clazz) {
		return spark
			.read()
			.textFile(inputPath)
			.map((MapFunction<String, R>) value -> OBJECT_MAPPER.readValue(value, clazz), Encoders.bean(clazz));
	}

}
