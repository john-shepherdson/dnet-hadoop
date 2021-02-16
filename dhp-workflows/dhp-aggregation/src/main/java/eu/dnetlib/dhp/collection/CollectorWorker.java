
package eu.dnetlib.dhp.collection;

import static eu.dnetlib.dhp.common.Constants.SEQUENCE_FILE_NAME;

import java.io.IOException;
import java.util.Optional;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.DeflateCodec;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import eu.dnetlib.data.mdstore.manager.common.model.MDStoreVersion;
import eu.dnetlib.dhp.collection.plugin.CollectorPlugin;
import eu.dnetlib.dhp.collection.plugin.mongodb.MongoDbCollectorPlugin;
import eu.dnetlib.dhp.collection.plugin.mongodb.MongoDbDumpCollectorPlugin;
import eu.dnetlib.dhp.collection.plugin.oai.OaiCollectorPlugin;

public class CollectorWorker {

	private static final Logger log = LoggerFactory.getLogger(CollectorWorker.class);
	public static final int ONGOING_REPORT_FREQUENCY_MS = 5000;

	private final ApiDescriptor api;

	private final FileSystem fileSystem;

	private final MDStoreVersion mdStoreVersion;

	private final HttpClientParams clientParams;

	private final CollectorPluginReport report;

	public CollectorWorker(
		final ApiDescriptor api,
		final FileSystem fileSystem,
		final MDStoreVersion mdStoreVersion,
		final HttpClientParams clientParams,
		final CollectorPluginReport report) {
		this.api = api;
		this.fileSystem = fileSystem;
		this.mdStoreVersion = mdStoreVersion;
		this.clientParams = clientParams;
		this.report = report;
	}

	public void collect() throws UnknownCollectorPluginException, CollectorException, IOException {

		final String outputPath = mdStoreVersion.getHdfsPath() + SEQUENCE_FILE_NAME;
		log.info("outputPath path is {}", outputPath);

		final CollectorPlugin plugin = getCollectorPlugin();
		final AtomicInteger counter = new AtomicInteger(0);

		final Timer timer = new Timer();
		timer.schedule(new TimerTask() {
			@Override
			public void run() {
				report.ongoing(counter.longValue(), null);
			}
		}, 5000, ONGOING_REPORT_FREQUENCY_MS);

		try (SequenceFile.Writer writer = SequenceFile
			.createWriter(
				fileSystem.getConf(),
				SequenceFile.Writer.file(new Path(outputPath)),
				SequenceFile.Writer.keyClass(IntWritable.class),
				SequenceFile.Writer.valueClass(Text.class),
				SequenceFile.Writer.compression(SequenceFile.CompressionType.BLOCK, new DeflateCodec()))) {
			final IntWritable key = new IntWritable(counter.get());
			final Text value = new Text();
			plugin
				.collect(api, report)
				.forEach(
					content -> {
						key.set(counter.getAndIncrement());
						value.set(content);
						try {
							writer.append(key, value);
						} catch (Throwable e) {
							throw new RuntimeException(e);
						}
					});
		} catch (Throwable e) {
			report.put(e.getClass().getName(), e.getMessage());
			throw new CollectorException(e);
		} finally {
			timer.cancel();
			report.ongoing(counter.longValue(), counter.longValue());
		}
	}

	private CollectorPlugin getCollectorPlugin() throws UnknownCollectorPluginException {
		switch (StringUtils.lowerCase(StringUtils.trim(api.getProtocol()))) {
			case "oai":
				return new OaiCollectorPlugin(clientParams);
			case "other":
				final String plugin = Optional
					.ofNullable(api.getParams().get("other_plugin_type"))
					.orElseThrow(() -> new UnknownCollectorPluginException("other_plugin_type"));

				switch (plugin) {
					case "mdstore_mongodb_dump":
						return new MongoDbDumpCollectorPlugin(fileSystem);
					case "mdstore_mongodb":
						return new MongoDbCollectorPlugin();
					default:
						throw new UnknownCollectorPluginException("Unknown plugin type: " + plugin);
				}
			default:
				throw new UnknownCollectorPluginException("Unknown protocol: " + api.getProtocol());
		}
	}

}
