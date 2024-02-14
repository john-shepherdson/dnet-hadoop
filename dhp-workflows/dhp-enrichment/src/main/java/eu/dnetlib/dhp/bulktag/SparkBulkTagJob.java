
package eu.dnetlib.dhp.bulktag;

import static eu.dnetlib.dhp.PropagationConstant.removeOutputDir;
import static eu.dnetlib.dhp.common.SparkSessionSupport.runWithSparkSession;

import java.util.*;
import java.util.stream.Collectors;

import eu.dnetlib.dhp.api.model.CommunityEntityMap;
import eu.dnetlib.dhp.api.model.EntityCommunities;
import eu.dnetlib.dhp.schema.common.ModelConstants;
import eu.dnetlib.dhp.schema.oaf.Context;
import eu.dnetlib.dhp.schema.oaf.Project;
import org.apache.commons.io.IOUtils;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.function.FilterFunction;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.gson.Gson;

import eu.dnetlib.dhp.api.Utils;
import eu.dnetlib.dhp.application.ArgumentApplicationParser;
import eu.dnetlib.dhp.bulktag.community.*;
import eu.dnetlib.dhp.schema.common.ModelSupport;
import eu.dnetlib.dhp.schema.oaf.Datasource;
import eu.dnetlib.dhp.schema.oaf.Result;
import eu.dnetlib.dhp.schema.oaf.utils.OafMapperUtils;
import scala.Tuple2;

public class SparkBulkTagJob {

	private static String OPENAIRE_3 = "openaire3.0";
	private static String OPENAIRE_4 = "openaire-pub_4.0";
	private static String OPENAIRE_CRIS = "openaire-cris_1.1";
	private static String OPENAIRE_DATA = "openaire2.0_data";
	private static String EOSC = "10|openaire____::2e06c1122c7df43765fdcf91080824fa";

	private static final Logger log = LoggerFactory.getLogger(SparkBulkTagJob.class);
	public static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

	public static void main(String[] args) throws Exception {
		String jsonConfiguration = IOUtils
			.toString(
				SparkBulkTagJob.class
					.getResourceAsStream(
						"/eu/dnetlib/dhp/wf/subworkflows/bulktag/input_bulkTag_parameters.json"));

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

		ProtoMap protoMappingParams = new Gson().fromJson(parser.get("pathMap"), ProtoMap.class);
		log.info("pathMap: {}", new Gson().toJson(protoMappingParams));

		SparkConf conf = new SparkConf();
		CommunityConfiguration cc;

		String taggingConf = Optional
			.ofNullable(parser.get("taggingConf"))
			.map(String::valueOf)
			.orElse(null);

		if (taggingConf != null) {
			cc = CommunityConfigurationFactory.newInstance(taggingConf);
		} else {
			cc = Utils.getCommunityConfiguration(baseURL);
			log.info(OBJECT_MAPPER.writeValueAsString(cc));
		}

		runWithSparkSession(
			conf,
			isSparkSessionManaged,
			spark -> {
				extendCommunityConfigurationForEOSC(spark, inputPath, cc);
				execBulkTag(spark, inputPath, outputPath, protoMappingParams, cc);
				execDatasourceTag(spark, inputPath, outputPath, Utils.getDatasourceCommunities(baseURL));
				execProjectTag(spark, inputPath, outputPath, Utils.getCommunityProjects(baseURL));
			});
	}

	private static void execProjectTag(SparkSession spark, String inputPath, String outputPath, CommunityEntityMap communityProjects) {
		Dataset<Project> projects = readPath(spark, inputPath + "project", Project.class);
		Dataset<EntityCommunities> pc = spark.createDataset(communityProjects.keySet().stream().map(k -> EntityCommunities.newInstance(k, communityProjects.get(k))).collect(Collectors.toList()), Encoders.bean(EntityCommunities.class));

		projects.joinWith(pc, projects.col("id").equalTo(pc.col("entityId")), "left")
				.map((MapFunction<Tuple2<Project, EntityCommunities>, Project>) t2 -> {
					Project ds = t2._1();
					if (t2._2() != null){
						List<String> context =
								Optional.ofNullable(ds.getContext())
										.map(v -> v.stream().map(c -> c.getId()).collect(Collectors.toList()))
										.orElse(new ArrayList<>());

						if(!Optional.ofNullable(ds.getContext()).isPresent())
							ds.setContext(new ArrayList<>());
						t2._2().getCommunitiesId().forEach(c -> {
							if(!context.contains(c)){
								Context con = new Context();
								con.setId(c);
								con.setDataInfo(Arrays.asList(OafMapperUtils.dataInfo(false,TaggingConstants.BULKTAG_DATA_INFO_TYPE, true, false,
										OafMapperUtils.qualifier(TaggingConstants.CLASS_ID_DATASOURCE, TaggingConstants.CLASS_NAME_BULKTAG_DATASOURCE, ModelConstants.DNET_PROVENANCE_ACTIONS, ModelConstants.DNET_PROVENANCE_ACTIONS), "1")));
								ds.getContext().add(con);
							}
						});
					}
					return ds;
				} ,Encoders.bean(Project.class))
				.write()
				.mode(SaveMode.Overwrite)
				.option("compression","gzip")
				.json(outputPath + "project");

		readPath(spark, outputPath + "project", Datasource.class)
				.write()
				.mode(SaveMode.Overwrite)
				.option("compression","gzip")
				.json(inputPath + "project");
	}


	private static void execDatasourceTag(SparkSession spark, String inputPath, String outputPath, List<EntityCommunities> datasourceCommunities) {
		Dataset<Datasource> datasource = readPath(spark, inputPath + "datasource", Datasource.class);

		Dataset<EntityCommunities> dc = spark.createDataset(datasourceCommunities, Encoders.bean(EntityCommunities.class));

		datasource.joinWith(dc, datasource.col("id").equalTo(dc.col("entityId")), "left")
				.map((MapFunction<Tuple2<Datasource, EntityCommunities>, Datasource>) t2 -> {
					Datasource ds = t2._1();
					if (t2._2() != null){

						List<String> context =
							Optional.ofNullable(ds.getContext())
									.map(v -> v.stream().map(c -> c.getId()).collect(Collectors.toList()))
									.orElse(new ArrayList<>());

						if(!Optional.ofNullable(ds.getContext()).isPresent())
							ds.setContext(new ArrayList<>());

						t2._2().getCommunitiesId().forEach(c -> {
							if(!context.contains(c)){
								Context con = new Context();
								con.setId(c);
								con.setDataInfo(Arrays.asList(OafMapperUtils.dataInfo(false,TaggingConstants.BULKTAG_DATA_INFO_TYPE, true, false,
										OafMapperUtils.qualifier(TaggingConstants.CLASS_ID_DATASOURCE, TaggingConstants.CLASS_NAME_BULKTAG_DATASOURCE, ModelConstants.DNET_PROVENANCE_ACTIONS, ModelConstants.DNET_PROVENANCE_ACTIONS), "1")));
								ds.getContext().add(con);
							}
						});
					}
					return ds;
				} ,Encoders.bean(Datasource.class))
				.write()
				.mode(SaveMode.Overwrite)
				.option("compression","gzip")
				.json(outputPath + "datasource");
		
		readPath(spark, outputPath + "datasource", Datasource.class)
				.write()
				.mode(SaveMode.Overwrite)
				.option("compression","gzip")
				.json(inputPath + "datasource");
	}


	private static void extendCommunityConfigurationForEOSC(SparkSession spark, String inputPath,
		CommunityConfiguration cc) {

		Dataset<String> datasources = readPath(
			spark, inputPath
				+ "datasource",
			Datasource.class)
				.filter((FilterFunction<Datasource>) ds -> isOKDatasource(ds))
				.map((MapFunction<Datasource, String>) ds -> ds.getId(), Encoders.STRING());

		Map<String, List<Pair<String, SelectionConstraints>>> dsm = cc.getEoscDatasourceMap();

		for (String ds : datasources.collectAsList()) {
			if (!dsm.containsKey(ds)) {
				ArrayList<Pair<String, SelectionConstraints>> eoscList = new ArrayList<>();
				dsm.put(ds, eoscList);
			}
		}

	}

	private static boolean isOKDatasource(Datasource ds) {
		final String compatibility = ds.getOpenairecompatibility().getClassid();
		return (compatibility.equalsIgnoreCase(OPENAIRE_3) ||
			compatibility.equalsIgnoreCase(OPENAIRE_4) ||
			compatibility.equalsIgnoreCase(OPENAIRE_CRIS) ||
			compatibility.equalsIgnoreCase(OPENAIRE_DATA)) &&
			ds.getCollectedfrom().stream().anyMatch(cf -> cf.getKey().equals(EOSC));
	}

	private static <R extends Result> void execBulkTag(
		SparkSession spark,
		String inputPath,
		String outputPath,
		ProtoMap protoMappingParams,
		CommunityConfiguration communityConfiguration) {

		ModelSupport.entityTypes
			.keySet()
			.parallelStream()
			.filter(ModelSupport::isResult)
			.forEach(e -> {
				removeOutputDir(spark, outputPath + e.name());
				ResultTagger resultTagger = new ResultTagger();
				Class<R> resultClazz = ModelSupport.entityTypes.get(e);
				readPath(spark, inputPath + e.name(), resultClazz)
					.map(patchResult(), Encoders.bean(resultClazz))
					.filter(Objects::nonNull)
					.map(
						(MapFunction<R, R>) value -> resultTagger
							.enrichContextCriteria(
								value, communityConfiguration, protoMappingParams),
						Encoders.bean(resultClazz))
					.write()
					.mode(SaveMode.Overwrite)
					.option("compression", "gzip")
					.json(outputPath + e.name());// writing the tagging in the working dir for entity

				readPath(spark, outputPath + e.name(), resultClazz) // copy the tagging in the actual result output path
					.write()
					.mode(SaveMode.Overwrite)
					.option("compression", "gzip")
					.json(inputPath + e.name());
			});

	}

	public static <R> Dataset<R> readPath(
		SparkSession spark, String inputPath, Class<R> clazz) {
		return spark
			.read()
			.textFile(inputPath)
			.map((MapFunction<String, R>) value -> OBJECT_MAPPER.readValue(value, clazz), Encoders.bean(clazz));
	}

	// TODO remove this hack as soon as the values fixed by this method will be provided as NON null
	private static <R extends Result> MapFunction<R, R> patchResult() {
		return r -> {
			if (Objects.isNull(r.getDataInfo())) {
				r.setDataInfo(OafMapperUtils.dataInfo(false, "", false, false, OafMapperUtils.unknown("", ""), ""));
			} else if (r.getDataInfo().getDeletedbyinference() == null) {
				r.getDataInfo().setDeletedbyinference(false);
			}
			if (Objects.isNull(r.getContext())) {
				r.setContext(new ArrayList<>());
			}
			return r;
		};
	}


}

