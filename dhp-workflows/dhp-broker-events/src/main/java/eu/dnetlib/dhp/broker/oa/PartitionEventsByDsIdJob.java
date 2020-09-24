
package eu.dnetlib.dhp.broker.oa;

import static eu.dnetlib.dhp.common.SparkSessionSupport.runWithSparkSession;

import java.io.IOException;
import java.util.Optional;

import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.spark.SparkConf;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.SaveMode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.gson.Gson;

import eu.dnetlib.broker.api.ShortEventMessage;
import eu.dnetlib.broker.objects.OaBrokerEventPayload;
import eu.dnetlib.dhp.application.ArgumentApplicationParser;
import eu.dnetlib.dhp.broker.model.Event;
import eu.dnetlib.dhp.broker.oa.util.ClusterUtils;
import scala.Tuple2;

public class PartitionEventsByDsIdJob {

	private static final Logger log = LoggerFactory.getLogger(PartitionEventsByDsIdJob.class);
	private static final String OPENDOAR_NSPREFIX = "opendoar____::";

	public static void main(final String[] args) throws Exception {

		final ArgumentApplicationParser parser = new ArgumentApplicationParser(
			IOUtils
				.toString(
					PartitionEventsByDsIdJob.class
						.getResourceAsStream("/eu/dnetlib/dhp/broker/oa/common_params.json")));
		parser.parseArgument(args);

		final Boolean isSparkSessionManaged = Optional
			.ofNullable(parser.get("isSparkSessionManaged"))
			.map(Boolean::valueOf)
			.orElse(Boolean.TRUE);
		log.info("isSparkSessionManaged: {}", isSparkSessionManaged);

		final SparkConf conf = new SparkConf();

		final String eventsPath = parser.get("workingPath") + "/events";
		log.info("eventsPath: {}", eventsPath);

		final String partitionPath = parser.get("workingPath") + "/eventsByOpendoarId";
		log.info("partitionPath: {}", partitionPath);

		runWithSparkSession(conf, isSparkSessionManaged, spark -> {

			ClusterUtils
				.readPath(spark, eventsPath, Event.class)
				.filter(e -> StringUtils.isNotBlank(e.getMap().getTargetDatasourceId()))
				.filter(e -> e.getMap().getTargetDatasourceId().contains(OPENDOAR_NSPREFIX))
				.map(
					e -> new Tuple2<>(
						StringUtils.substringAfter(e.getMap().getTargetDatasourceId(), OPENDOAR_NSPREFIX),
						messageFromNotification(e)),
					Encoders.tuple(Encoders.STRING(), Encoders.bean(ShortEventMessage.class)))
				.write()
				.partitionBy("_1")
				.mode(SaveMode.Overwrite)
				.json(partitionPath);

		});
		renameSubDirs(partitionPath);

	}

	private static void renameSubDirs(final String path) throws IOException {
		final String prefix = "_1=";
		final FileSystem fs = FileSystem.get(new Configuration());

		log.info("** Renaming subdirs of " + path);
		for (final FileStatus fileStatus : fs.listStatus(new Path(path))) {
			if (fileStatus.isDirectory()) {
				final Path oldPath = fileStatus.getPath();
				final String oldName = oldPath.getName();
				if (oldName.startsWith(prefix)) {
					final Path newPath = new Path(path + "/" + StringUtils.substringAfter(oldName, prefix));
					log.info(" * " + oldPath.getName() + " -> " + newPath.getName());
					fs.rename(oldPath, newPath);
				}
			}
		}
	}

	private static ShortEventMessage messageFromNotification(final Event e) {
		final Gson gson = new Gson();

		final OaBrokerEventPayload payload = gson.fromJson(e.getPayload(), OaBrokerEventPayload.class);

		final ShortEventMessage res = new ShortEventMessage();

		res.setOriginalId(payload.getResult().getOriginalId());
		res.setTitle(payload.getResult().getTitles().stream().filter(StringUtils::isNotBlank).findFirst().orElse(null));
		res.setTopic(e.getTopic());
		res.setTrust(payload.getTrust());
		res.generateMessageFromObject(payload.getHighlight());

		return res;
	}

}
