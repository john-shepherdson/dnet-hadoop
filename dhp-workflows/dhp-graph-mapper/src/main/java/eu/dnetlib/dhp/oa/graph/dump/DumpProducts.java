
package eu.dnetlib.dhp.oa.graph.dump;

import static eu.dnetlib.dhp.common.SparkSessionSupport.runWithSparkSession;

import java.io.Serializable;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

import org.apache.spark.SparkConf;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;

import eu.dnetlib.dhp.oa.graph.dump.ResultMapper;
import eu.dnetlib.dhp.oa.graph.dump.Utils;
import eu.dnetlib.dhp.oa.graph.dump.community.CommunityMap;
import eu.dnetlib.dhp.schema.dump.oaf.graph.ResearchInitiative;
import eu.dnetlib.dhp.schema.oaf.*;

public class DumpProducts implements Serializable {

	public void run(Boolean isSparkSessionManaged, String inputPath, String outputPath, CommunityMap communityMap,
		Class<? extends OafEntity> inputClazz,
		Class<? extends eu.dnetlib.dhp.schema.dump.oaf.Result> outputClazz,
		boolean graph) {

		SparkConf conf = new SparkConf();

		runWithSparkSession(
			conf,
			isSparkSessionManaged,
			spark -> {
				Utils.removeOutputDir(spark, outputPath);
				execDump(spark, inputPath, outputPath, communityMap, inputClazz, outputClazz, graph);// , dumpClazz);

			});
	}

	public static <I extends OafEntity, O extends eu.dnetlib.dhp.schema.dump.oaf.Result> void execDump(
		SparkSession spark,
		String inputPath,
		String outputPath,
		CommunityMap communityMap,
		Class<I> inputClazz,
		Class<O> outputClazz,
		boolean graph) throws ClassNotFoundException {

		Utils
			.readPath(spark, inputPath, inputClazz)
			.map(value -> execMap(value, communityMap, graph), Encoders.bean(outputClazz))
			.filter(Objects::nonNull)
			.write()
			.mode(SaveMode.Overwrite)
			.option("compression", "gzip")
			.json(outputPath);

	}

	private static <I extends OafEntity, O extends eu.dnetlib.dhp.schema.dump.oaf.Result> O execMap(I value,
		CommunityMap communityMap,
		boolean graph) {

		Optional<DataInfo> odInfo = Optional.ofNullable(value.getDataInfo());
		if (odInfo.isPresent()) {
			if (odInfo.get().getDeletedbyinference()) {
				return null;
			}
		} else {
			return null;
		}

		if (!graph) {
			Set<String> communities = communityMap.keySet();

			Optional<List<Context>> inputContext = Optional
				.ofNullable(((eu.dnetlib.dhp.schema.oaf.Result) value).getContext());
			if (!inputContext.isPresent()) {
				return null;
			}
			List<String> toDumpFor = inputContext.get().stream().map(c -> {
				if (communities.contains(c.getId())) {
					return c.getId();
				}
				if (c.getId().contains("::") && communities.contains(c.getId().substring(0, c.getId().indexOf("::")))) {
					return c.getId().substring(0, 3);
				}
				return null;
			}).filter(Objects::nonNull).collect(Collectors.toList());
			if (toDumpFor.size() == 0) {
				return null;
			}
		}
		return (O) ResultMapper.map(value, communityMap, graph);

	}
}
