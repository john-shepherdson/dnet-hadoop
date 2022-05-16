
package eu.dnetlib.dhp.bulktag;

import static eu.dnetlib.dhp.PropagationConstant.readPath;
import static eu.dnetlib.dhp.common.SparkSessionSupport.runWithSparkSession;

import java.util.*;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

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
import eu.dnetlib.dhp.schema.common.ModelConstants;
import eu.dnetlib.dhp.schema.oaf.*;
import eu.dnetlib.dhp.schema.oaf.utils.OafMapperUtils;

public class SparkEoscTag {
	private static final Logger log = LoggerFactory.getLogger(SparkEoscTag.class);
	public static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();
	public static final Qualifier EOSC_QUALIFIER = OafMapperUtils
		.qualifier(
			"EOSC",
			"European Open Science Cloud",
			ModelConstants.DNET_SUBJECT_TYPOLOGIES, ModelConstants.DNET_SUBJECT_TYPOLOGIES);
	public static final DataInfo EOSC_DATAINFO = OafMapperUtils
		.dataInfo(
			false, "propagation", true, false,
			OafMapperUtils
				.qualifier(
					"propagation:subject", "Inferred by OpenAIRE",
					ModelConstants.DNET_PROVENANCE_ACTIONS, ModelConstants.DNET_PROVENANCE_ACTIONS),
			"0.9");
	public final static StructuredProperty EOSC_NOTEBOOK = OafMapperUtils
		.structuredProperty(
			"EOSC::Jupyter Notebook", EOSC_QUALIFIER, EOSC_DATAINFO);
	public final static StructuredProperty EOSC_GALAXY = OafMapperUtils
		.structuredProperty(
			"EOSC::Galaxy Workflow", EOSC_QUALIFIER, EOSC_DATAINFO);
	public final static StructuredProperty EOSC_TWITTER = OafMapperUtils
		.structuredProperty(
			"EOSC::Twitter Data", EOSC_QUALIFIER, EOSC_DATAINFO);

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

	private static void execEoscTag(SparkSession spark, String inputPath, String workingPath) {

		readPath(spark, inputPath + "/software", Software.class)
			.map((MapFunction<Software, Software>) s -> {
				List<StructuredProperty> sbject;
				if (!Optional.ofNullable(s.getSubject()).isPresent())
					s.setSubject(new ArrayList<>());
				sbject = s.getSubject();

				if (containsCriteriaNotebook(s)) {
					sbject.add(EOSC_NOTEBOOK);
					if (sbject.stream().anyMatch(sb -> sb.getValue().equals("EOSC Jupyter Notebook"))){
						sbject = sbject.stream().map(sb -> {
							if (sb.getValue().equals("EOSC Jupyter Notebook")){
								return null;
							}
							return sb;
						}).filter(Objects::nonNull).collect(Collectors.toList());
					}
				}
				if (containsCriteriaGalaxy(s)) {
					sbject.add(EOSC_GALAXY);
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
				List<StructuredProperty> sbject;
				if (!Optional.ofNullable(orp.getSubject()).isPresent())
					orp.setSubject(new ArrayList<>());
				sbject = orp.getSubject();
				if (containsCriteriaGalaxy(orp)) {
					sbject.add(EOSC_GALAXY);
				}
				if (containscriteriaTwitter(orp)) {
					sbject.add(EOSC_TWITTER);
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
				List<StructuredProperty> sbject;
				if (!Optional.ofNullable(d.getSubject()).isPresent())
					d.setSubject(new ArrayList<>());
				sbject = d.getSubject();
				if (containscriteriaTwitter(d)) {
					sbject.add(EOSC_TWITTER);
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

	private static boolean containscriteriaTwitter(Result r) {
		Set<String> words = getWordsSP(r.getTitle());
		words.addAll(getWordsF(r.getDescription()));

		if (words.contains("twitter") &&
			(words.contains("data") || words.contains("dataset")))
			return true;

		if (r.getSubject().stream().anyMatch(sbj -> sbj.getValue().toLowerCase().contains("twitter")) &&
			r.getSubject().stream().anyMatch(sbj -> sbj.getValue().toLowerCase().contains("data")))
			return true;
		return false;
	}

	private static boolean containsCriteriaGalaxy(Result r) {
		Set<String> words = getWordsSP(r.getTitle());
		words.addAll(getWordsF(r.getDescription()));
		if (words.contains("galaxy") &&
			words.contains("workflow"))
			return true;

		if (r.getSubject().stream().anyMatch(sbj -> sbj.getValue().toLowerCase().contains("galaxy")) &&
			r.getSubject().stream().anyMatch(sbj -> sbj.getValue().toLowerCase().contains("workflow")))
			return true;
		return false;
	}

	private static boolean containsCriteriaNotebook(Software s) {
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

	private static Set<String> getSubjects(List<StructuredProperty> s) {
		Set<String> subjects = new HashSet<>();
		s.stream().forEach(sbj -> subjects.addAll(Arrays.asList(sbj.getValue().toLowerCase().split(" "))));
		s.stream().forEach(sbj -> subjects.add(sbj.getValue().toLowerCase()));
		return subjects;
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
//		elem
//			.forEach(
//				t -> words.addAll(Arrays.asList(t.getValue().toLowerCase().replaceAll("[^a-zA-Z ]", "").split(" "))));
		return words;

	}
}
