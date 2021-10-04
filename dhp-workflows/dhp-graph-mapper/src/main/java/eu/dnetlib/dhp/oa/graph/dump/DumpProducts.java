
package eu.dnetlib.dhp.oa.graph.dump;

import static eu.dnetlib.dhp.common.SparkSessionSupport.runWithSparkSession;

import java.io.Serializable;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;

import eu.dnetlib.dhp.oa.graph.dump.community.CommunityMap;
import eu.dnetlib.dhp.oa.graph.dump.exceptions.NoAvailableEntityTypeException;
import eu.dnetlib.dhp.schema.oaf.*;

/**
 * It fires the execution of the actual dump for result entities. If the dump is for RC/RI products its checks for each
 * result its belongingess to at least one RC/RI before "asking" for its mapping.
 */
public class DumpProducts implements Serializable {

	public void run(Boolean isSparkSessionManaged, String inputPath, String outputPath, String communityMapPath,
		Class<? extends OafEntity> inputClazz,
		Class<? extends eu.dnetlib.dhp.schema.dump.oaf.Result> outputClazz,
		String dumpType) {

		SparkConf conf = new SparkConf();

		runWithSparkSession(
			conf,
			isSparkSessionManaged,
			spark -> {
				Utils.removeOutputDir(spark, outputPath);
				execDump(
					spark, inputPath, outputPath, communityMapPath, inputClazz, outputClazz, dumpType);
			});
	}

	public static <I extends OafEntity, O extends eu.dnetlib.dhp.schema.dump.oaf.Result> void execDump(
		SparkSession spark,
		String inputPath,
		String outputPath,
		String communityMapPath,
		Class<I> inputClazz,
		Class<O> outputClazz,
		String dumpType) {

		CommunityMap communityMap = Utils.getCommunityMap(spark, communityMapPath);

		Utils
			.readPath(spark, inputPath, inputClazz)
			.map((MapFunction<I, O>) value -> execMap(value, communityMap, dumpType), Encoders.bean(outputClazz))
			.filter(Objects::nonNull)
			.write()
			.mode(SaveMode.Overwrite)
			.option("compression", "gzip")
			.json(outputPath);

	}

	private static <I extends OafEntity, O extends eu.dnetlib.dhp.schema.dump.oaf.Result> O execMap(I value,
		CommunityMap communityMap,
		String dumpType) throws NoAvailableEntityTypeException {

		Optional<DataInfo> odInfo = Optional.ofNullable(value.getDataInfo());
		if (odInfo.isPresent()) {
			if (odInfo.get().getDeletedbyinference() || odInfo.get().getInvisible()) {
				return null;
			}
		} else {
			return null;
		}

		if (Constants.DUMPTYPE.COMMUNITY.getType().equals(dumpType)) {
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
					return c.getId().substring(0, c.getId().indexOf("::"));
				}
				return null;
			}).filter(Objects::nonNull).collect(Collectors.toList());
			if (toDumpFor.isEmpty()) {
				return null;
			}
		}

		return (O) ResultMapper.map(value, communityMap, dumpType);

	}
}
