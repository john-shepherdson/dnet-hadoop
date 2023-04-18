
package eu.dnetlib.dhp.bulktag.eosc;

import static eu.dnetlib.dhp.PropagationConstant.readPath;
import static eu.dnetlib.dhp.common.SparkSessionSupport.runWithSparkSession;

import java.util.*;

import org.apache.commons.io.IOUtils;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.databind.ObjectMapper;

import eu.dnetlib.dhp.application.ArgumentApplicationParser;
import eu.dnetlib.dhp.schema.oaf.*;

public class SparkEoscTag {
	private static final Logger log = LoggerFactory.getLogger(SparkEoscTag.class);
	public static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();
	public static final String EOSC_GALAXY_WORKFLOW = "EOSC::Galaxy Workflow";
	public static final String EOSC_TWITTER_DATA = "EOSC::Twitter Data";
	public static final String EOSC_JUPYTER_NOTEBOOK = "EOSC::Jupyter Notebook";
	public static final String COMPLIES_WITH = "compliesWith";

	public static void main(String[] args) throws Exception {
		String jsonConfiguration = IOUtils
			.toString(
				SparkEoscTag.class
					.getResourceAsStream(
						"/eu/dnetlib/dhp/bulktag/input_eoscTag_parameters.json"));

		final ArgumentApplicationParser parser = new ArgumentApplicationParser(jsonConfiguration);
		parser.parseArgument(args);

		Boolean isSparkSessionManaged = Optional
			.ofNullable(parser.get("isSparkSessionManaged"))
			.map(Boolean::valueOf)
			.orElse(Boolean.TRUE);
		log.info("isSparkSessionManaged: {}", isSparkSessionManaged);

		final String inputPath = parser.get("sourcePath");
		log.info("inputPath: {}", inputPath);

		final String workingPath = parser.get("workingPath");
		log.info("workingPath: {}", workingPath);

		SparkConf conf = new SparkConf();

		runWithSparkSession(
			conf,
			isSparkSessionManaged,
			spark -> {
				execEoscTag(spark, inputPath, workingPath);

			});
	}

	public static EoscIfGuidelines newInstance(String code, String label, String url, String semantics) {
		EoscIfGuidelines eig = new EoscIfGuidelines();
		eig.setCode(code);
		eig.setLabel(label);
		eig.setUrl(url);
		eig.setSemanticRelation(semantics);
		return eig;

	}

	public static <R extends Result> R tagForSoftware(Result s){
		if (containsCriteriaNotebook(s)) {
			if (!Optional.ofNullable(s.getEoscifguidelines()).isPresent())
				s.setEoscifguidelines(new ArrayList<>());
			addEIG(
					s.getEoscifguidelines(), EOSC_JUPYTER_NOTEBOOK, EOSC_JUPYTER_NOTEBOOK, "",
					COMPLIES_WITH);

		}
		if (containsCriteriaGalaxy(s)) {
			if (!Optional.ofNullable(s.getEoscifguidelines()).isPresent())
				s.setEoscifguidelines(new ArrayList<>());

			addEIG(
					s.getEoscifguidelines(), EOSC_GALAXY_WORKFLOW, EOSC_GALAXY_WORKFLOW, "", COMPLIES_WITH);
		}
		return s;
	}

	private static void execEoscTag(SparkSession spark, String inputPath, String workingPath) {

		readPath(spark, inputPath + "/software", Software.class)
			.map((MapFunction<Software, Software>) s -> {

				if (containsCriteriaNotebook(s)) {
					if (!Optional.ofNullable(s.getEoscifguidelines()).isPresent())
						s.setEoscifguidelines(new ArrayList<>());
					addEIG(
						s.getEoscifguidelines(), EOSC_JUPYTER_NOTEBOOK, EOSC_JUPYTER_NOTEBOOK, "",
						COMPLIES_WITH);

				}
				if (containsCriteriaGalaxy(s)) {
					if (!Optional.ofNullable(s.getEoscifguidelines()).isPresent())
						s.setEoscifguidelines(new ArrayList<>());

					addEIG(
						s.getEoscifguidelines(), EOSC_GALAXY_WORKFLOW, EOSC_GALAXY_WORKFLOW, "", COMPLIES_WITH);
				}
				return s;
			}, Encoders.bean(Software.class))
			.write()
			.mode(SaveMode.Overwrite)
			.option("compression", "gzip")
			.json(workingPath + "/software");

		readPath(spark, workingPath + "/software", Software.class)
			.write()
			.mode(SaveMode.Overwrite)
			.option("compression", "gzip")
			.json(inputPath + "/software");

		readPath(spark, inputPath + "/otherresearchproduct", OtherResearchProduct.class)
			.map((MapFunction<OtherResearchProduct, OtherResearchProduct>) orp -> {

				if (!Optional.ofNullable(orp.getEoscifguidelines()).isPresent())
					orp.setEoscifguidelines(new ArrayList<>());

				if (containsCriteriaGalaxy(orp)) {
					addEIG(
						orp.getEoscifguidelines(), EOSC_GALAXY_WORKFLOW, EOSC_GALAXY_WORKFLOW, "",
						COMPLIES_WITH);
				}
				if (containscriteriaTwitter(orp)) {
					addEIG(orp.getEoscifguidelines(), EOSC_TWITTER_DATA, EOSC_TWITTER_DATA, "", COMPLIES_WITH);
				}
				return orp;
			}, Encoders.bean(OtherResearchProduct.class))
			.write()
			.mode(SaveMode.Overwrite)
			.option("compression", "gzip")
			.json(workingPath + "/otherresearchproduct");

		readPath(spark, workingPath + "/otherresearchproduct", OtherResearchProduct.class)
			.write()
			.mode(SaveMode.Overwrite)
			.option("compression", "gzip")
			.json(inputPath + "/otherresearchproduct");

		readPath(spark, inputPath + "/dataset", Dataset.class)
			.map((MapFunction<Dataset, Dataset>) d -> {

				if (!Optional.ofNullable(d.getEoscifguidelines()).isPresent())
					d.setEoscifguidelines(new ArrayList<>());
				if (containscriteriaTwitter(d)) {
					addEIG(d.getEoscifguidelines(), EOSC_TWITTER_DATA, EOSC_TWITTER_DATA, "", COMPLIES_WITH);
				}
				return d;
			}, Encoders.bean(Dataset.class))
			.write()
			.mode(SaveMode.Overwrite)
			.option("compression", "gzip")
			.json(workingPath + "/dataset");

		readPath(spark, workingPath + "/dataset", Dataset.class)
			.write()
			.mode(SaveMode.Overwrite)
			.option("compression", "gzip")
			.json(inputPath + "/dataset");
	}

	private static void addEIG(List<EoscIfGuidelines> eoscifguidelines, String code, String label, String url,
		String sem) {
		if (!eoscifguidelines.stream().anyMatch(eig -> eig.getCode().equals(code)))
			eoscifguidelines.add(newInstance(code, label, url, sem));
	}

	private static boolean containscriteriaTwitter(Result r) {
		Set<String> words = getWordsSP(r.getTitle());
		words.addAll(getWordsF(r.getDescription()));

		if (words.contains("twitter") &&
			(words.contains("data") || words.contains("dataset")))
			return true;

		return Optional
			.ofNullable(r.getSubject())
			.map(
				s -> s.stream().anyMatch(sbj -> sbj.getValue().toLowerCase().contains("twitter")) &&
					s.stream().anyMatch(sbj -> sbj.getValue().toLowerCase().contains("data")))
			.orElse(false);
	}

	private static boolean containsCriteriaGalaxy(Result r) {
		Set<String> words = getWordsSP(r.getTitle());
		words.addAll(getWordsF(r.getDescription()));
		if (words.contains("galaxy") &&
			words.contains("workflow"))
			return true;

		return Optional
			.ofNullable(r.getSubject())
			.map(
				s -> s.stream().anyMatch(sbj -> sbj.getValue().toLowerCase().contains("galaxy")) &&
					s.stream().anyMatch(sbj -> sbj.getValue().toLowerCase().contains("workflow")))
			.orElse(false);
	}

	private static <R extends Result> boolean containsCriteriaNotebook(R s) {
		if (!Optional.ofNullable(s.getSubject()).isPresent())
			return false;
		if (s.getSubject().stream().anyMatch(sbj -> sbj.getValue().toLowerCase().contains("jupyter")))
			return true;
		if (s
			.getSubject()
			.stream()
			.anyMatch(
				sbj -> sbj.getValue().toLowerCase().contains("python") &&
					sbj.getValue().toLowerCase().contains("notebook")))
			return true;
		if (s.getSubject().stream().anyMatch(sbj -> sbj.getValue().toLowerCase().contains("python")) &&
			s.getSubject().stream().anyMatch(sbj -> sbj.getValue().toLowerCase().contains("notebook")))
			return true;
		return false;
	}

	private static Set<String> getWordsSP(List<StructuredProperty> elem) {
		Set<String> words = new HashSet<>();
		Optional
			.ofNullable(elem)
			.ifPresent(
				e -> e
					.forEach(
						t -> words
							.addAll(
								Arrays.asList(t.getValue().toLowerCase().replaceAll("[^a-zA-Z ]", "").split(" ")))));
		return words;
	}

	private static Set<String> getWordsF(List<Field<String>> elem) {
		Set<String> words = new HashSet<>();
		Optional
			.ofNullable(elem)
			.ifPresent(
				e -> e
					.forEach(
						t -> words
							.addAll(
								Arrays.asList(t.getValue().toLowerCase().replaceAll("[^a-zA-Z ]", "").split(" ")))));

		return words;
	}
}
