
package eu.dnetlib.dhp.broker.oa;

import static eu.dnetlib.dhp.common.SparkSessionSupport.runWithSparkSession;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

import org.apache.commons.codec.digest.DigestUtils;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.function.FilterFunction;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.SaveMode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.gson.Gson;

import eu.dnetlib.broker.objects.OaBrokerEventPayload;
import eu.dnetlib.dhp.application.ArgumentApplicationParser;
import eu.dnetlib.dhp.broker.model.Event;
import eu.dnetlib.dhp.broker.model.ShortEventMessageWithGroupId;
import eu.dnetlib.dhp.broker.oa.util.ClusterUtils;

public class PartitionEventsByDsIdJob {

	private static final Logger log = LoggerFactory.getLogger(PartitionEventsByDsIdJob.class);
	private static final String OPENDOAR_NSPREFIX = "opendoar____::";

	public static void main(final String[] args) throws Exception {

		final ArgumentApplicationParser parser = new ArgumentApplicationParser(
			IOUtils
				.toString(
					PartitionEventsByDsIdJob.class
						.getResourceAsStream("/eu/dnetlib/dhp/broker/oa/od_partitions_params.json")));
		parser.parseArgument(args);

		final Boolean isSparkSessionManaged = Optional
			.ofNullable(parser.get("isSparkSessionManaged"))
			.map(Boolean::valueOf)
			.orElse(Boolean.TRUE);
		log.info("isSparkSessionManaged: {}", isSparkSessionManaged);

		final SparkConf conf = new SparkConf();

		final String eventsPath = parser.get("outputDir") + "/events";
		log.info("eventsPath: {}", eventsPath);

		final String partitionPath = parser.get("outputDir") + "/eventsByOpendoarId";
		log.info("partitionPath: {}", partitionPath);

		final String opendoarIds = parser.get("opendoarIds");
		log.info("opendoarIds: {}", opendoarIds);

		final Set<String> validOpendoarIds = new HashSet<>();
		if (!opendoarIds.trim().equals("-")) {
			validOpendoarIds
				.addAll(
					Arrays
						.stream(opendoarIds.split(","))
						.map(String::trim)
						.filter(StringUtils::isNotBlank)
						.map(s -> OPENDOAR_NSPREFIX + DigestUtils.md5Hex(s))
						.collect(Collectors.toSet()));
		}
		log.info("validOpendoarIds: {}", validOpendoarIds);

		runWithSparkSession(
			conf, isSparkSessionManaged, spark -> ClusterUtils
				.readPath(spark, eventsPath, Event.class)
				.filter((FilterFunction<Event>) e -> StringUtils.isNotBlank(e.getMap().getTargetDatasourceId()))
				.filter((FilterFunction<Event>) e -> e.getMap().getTargetDatasourceId().startsWith(OPENDOAR_NSPREFIX))
				.filter((FilterFunction<Event>) e -> validOpendoarIds.contains(e.getMap().getTargetDatasourceId()))
				.map(
					(MapFunction<Event, ShortEventMessageWithGroupId>) e -> messageFromNotification(e),
					Encoders.bean(ShortEventMessageWithGroupId.class))
				.coalesce(1)
				.write()
				.partitionBy("group")
				.mode(SaveMode.Overwrite)
				.option("compression", "gzip")
				.json(partitionPath));
		renameSubDirs(partitionPath);

	}

	private static void renameSubDirs(final String path) throws IOException {
		final FileSystem fs = FileSystem.get(new Configuration());

		log.info("** Renaming subdirs of {}", path);
		for (final FileStatus fileStatus : fs.listStatus(new Path(path))) {
			if (fileStatus.isDirectory()) {
				final Path oldPath = fileStatus.getPath();
				final String oldName = oldPath.getName();
				if (oldName.contains("=")) {
					final Path newPath = new Path(path + "/" + StringUtils.substringAfter(oldName, "="));
					log.info(" * {} -> {}", oldPath.getName(), newPath.getName());
					fs.rename(oldPath, newPath);
				}
			}
		}
	}

	private static ShortEventMessageWithGroupId messageFromNotification(final Event e) {
		final Gson gson = new Gson();

		final OaBrokerEventPayload payload = gson.fromJson(e.getPayload(), OaBrokerEventPayload.class);

		final ShortEventMessageWithGroupId res = new ShortEventMessageWithGroupId();

		res.setEventId(e.getEventId());
		res.setOriginalId(payload.getResult().getOriginalId());
		res.setTitle(payload.getResult().getTitles().stream().filter(StringUtils::isNotBlank).findFirst().orElse(null));
		res.setTopic(e.getTopic());
		res.setTrust(payload.getTrust());
		res.generateMessageFromObject(payload.getHighlight());
		res.setGroup(StringUtils.substringAfter(e.getMap().getTargetDatasourceId(), OPENDOAR_NSPREFIX));

		return res;
	}

}
