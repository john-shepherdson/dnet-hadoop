
package eu.dnetlib.dhp.oa.graph.dump.graph;

import static eu.dnetlib.dhp.common.SparkSessionSupport.runWithSparkSession;

import java.io.Serializable;
import java.util.*;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;

import eu.dnetlib.dhp.oa.graph.dump.DumpProducts;
import eu.dnetlib.dhp.oa.graph.dump.Utils;
import eu.dnetlib.dhp.oa.graph.dump.community.CommunityMap;
import eu.dnetlib.dhp.schema.common.ModelConstants;
import eu.dnetlib.dhp.schema.common.ModelSupport;
import eu.dnetlib.dhp.schema.dump.oaf.Provenance;
import eu.dnetlib.dhp.schema.dump.oaf.graph.Node;
import eu.dnetlib.dhp.schema.dump.oaf.graph.RelType;
import eu.dnetlib.dhp.schema.dump.oaf.graph.Relation;
import eu.dnetlib.dhp.schema.oaf.KeyValue;
import eu.dnetlib.dhp.schema.oaf.OafEntity;
import eu.dnetlib.dhp.schema.oaf.Project;
import eu.dnetlib.dhp.schema.oaf.Result;

public class Extractor implements Serializable {

	public void run(Boolean isSparkSessionManaged,
		String inputPath,
		String outputPath,
		Class<? extends Result> inputClazz,
		CommunityMap communityMap) {

		SparkConf conf = new SparkConf();

		runWithSparkSession(
			conf,
			isSparkSessionManaged,
			spark -> {
				Utils.removeOutputDir(spark, outputPath);
				extractRelationResult(spark, inputPath, outputPath, inputClazz, communityMap);
			});
	}

//    private static void extractRelationProjects(SparkSession spark, String inputPath, String outputPath){
//        Utils.readPath(spark, inputPath, Project.class)
//                .flatMap((FlatMapFunction<Project, Relation>) project ->{
//                    List<Relation> relList = new ArrayList<>();
//                    Optional.ofNullable(project.getCollectedfrom())
//                            .ifPresent(cfl ->
//                                    cfl.forEach(cf -> {
//                                        Provenance provenance = Provenance.newInstance(cf.getDataInfo().getProvenanceaction().getClassname(),
//                                                cf.getDataInfo().getTrust());
//
//                                        relList.add(getRelation(project.getId(), cf.getKey(),
//                                                Constants.PROJECT_ENTITY, Constants.DATASOURCE_ENTITY, Constants.IS_FUNDED_BY,
//                                                Constants.FUNDINGS, provenance));
//                                        relList.add(getRelation(cf.getKey(), project.getId(),
//                                                Constants.DATASOURCE_ENTITY, Constants.PROJECT_ENTITY, Constants.FUNDS,
//                                                Constants.FUNDINGS, provenance));
//                                    }));
//                    return relList.iterator();
//                }, Encoders.bean(Relation.class))
//                .write()
//                .option("Compression", "gzip")
//                .mode(SaveMode.Append)
//                .json(outputPath);
//    }

	private <R extends Result> void extractRelationResult(SparkSession spark,
		String inputPath,
		String outputPath,
		Class<R> inputClazz,
		CommunityMap communityMap) {

		Set<Integer> hashCodes = new HashSet<>();

		Utils
			.readPath(spark, inputPath, inputClazz)
			.flatMap((FlatMapFunction<R, Relation>) value -> {
				List<Relation> relationList = new ArrayList<>();
				Optional
					.ofNullable(value.getInstance())
					.ifPresent(inst -> inst.forEach(instance -> {
						Optional
							.ofNullable(instance.getCollectedfrom())
							.ifPresent(
								cf -> getRelatioPair(
									value, relationList, cf,
									ModelConstants.IS_PROVIDED_BY, ModelConstants.PROVIDES, hashCodes));
						Optional
							.ofNullable(instance.getHostedby())
							.ifPresent(
								hb -> getRelatioPair(
									value, relationList, hb,
									Constants.IS_HOSTED_BY, Constants.HOSTS, hashCodes));
					}));
				Set<String> communities = communityMap.keySet();
				Optional
					.ofNullable(value.getContext())
					.ifPresent(contexts -> contexts.forEach(context -> {
						String id = context.getId();
						if (id.contains(":")) {
							id = id.substring(0, id.indexOf(":"));
						}
						if (communities.contains(id)) {
							String contextId = Utils.getContextId(id);
							Provenance provenance = Optional
								.ofNullable(context.getDataInfo())
								.map(
									dinfo -> Optional
										.ofNullable(dinfo.get(0).getProvenanceaction())
										.map(
											paction -> Provenance
												.newInstance(
													paction.getClassid(),
													dinfo.get(0).getTrust()))
										.orElse(null))
								.orElse(null);
							Relation r = getRelation(
								value.getId(), contextId,
								Constants.RESULT_ENTITY,
								Constants.CONTEXT_ENTITY,
								ModelConstants.RELATIONSHIP, ModelConstants.IS_RELATED_TO, provenance);
							if (!hashCodes.contains(r.hashCode())) {
								relationList
									.add(r);
								hashCodes.add(r.hashCode());
							}
							r = getRelation(
								contextId, value.getId(),
								Constants.CONTEXT_ENTITY,
								Constants.RESULT_ENTITY,
								ModelConstants.RELATIONSHIP,
								ModelConstants.IS_RELATED_TO, provenance);
							if (!hashCodes.contains(r.hashCode())) {
								relationList
									.add(
										r);
								hashCodes.add(r.hashCode());
							}

						}

					}));

				return relationList.iterator();
			}, Encoders.bean(Relation.class))
			.write()
			.option("compression", "gzip")
			.mode(SaveMode.Overwrite)
			.json(outputPath);

	}

	private static <R extends Result> void getRelatioPair(R value, List<Relation> relationList, KeyValue cf,
		String result_dtasource, String datasource_result,
		Set<Integer> hashCodes) {
		Provenance provenance = Optional
			.ofNullable(cf.getDataInfo())
			.map(
				dinfo -> Optional
					.ofNullable(dinfo.getProvenanceaction())
					.map(
						paction -> Provenance
							.newInstance(
								paction.getClassid(),
								dinfo.getTrust()))
					.orElse(Provenance.newInstance(Constants.HARVESTED, Constants.DEFAULT_TRUST)))
			.orElse(Provenance.newInstance(Constants.HARVESTED, Constants.DEFAULT_TRUST));
		Relation r = getRelation(
			value.getId(),
			cf.getKey(), Constants.RESULT_ENTITY, Constants.DATASOURCE_ENTITY,
			result_dtasource, ModelConstants.PROVISION,
			provenance);
		if (!hashCodes.contains(r.hashCode())) {
			relationList
				.add(r);
			hashCodes.add(r.hashCode());
		}

		r = getRelation(
			cf.getKey(), value.getId(),
			Constants.DATASOURCE_ENTITY, Constants.RESULT_ENTITY,
			datasource_result, ModelConstants.PROVISION,
			provenance);

		if (!hashCodes.contains(r.hashCode())) {
			relationList
				.add(r);
			hashCodes.add(r.hashCode());
		}

	}

	private static Relation getRelation(String source, String target, String sourceType, String targetType,
		String relName, String relType, Provenance provenance) {
		Relation r = new Relation();
		r.setSource(Node.newInstance(source, sourceType));
		r.setTarget(Node.newInstance(target, targetType));
		r.setReltype(RelType.newInstance(relName, relType));
		r.setProvenance(provenance);
		return r;
	}
}
