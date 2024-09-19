
package eu.dnetlib.dhp.collection;

import static eu.dnetlib.dhp.common.Constants.SEQUENCE_FILE_NAME;

import java.io.IOException;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.DeflateCodec;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import eu.dnetlib.dhp.aggregation.common.ReporterCallback;
import eu.dnetlib.dhp.aggregation.common.ReportingJob;
import eu.dnetlib.dhp.collection.plugin.CollectorPlugin;
import eu.dnetlib.dhp.collection.plugin.base.BaseCollectorPlugin;
import eu.dnetlib.dhp.collection.plugin.file.FileCollectorPlugin;
import eu.dnetlib.dhp.collection.plugin.file.FileGZipCollectorPlugin;
import eu.dnetlib.dhp.collection.plugin.gtr2.Gtr2PublicationsCollectorPlugin;
import eu.dnetlib.dhp.collection.plugin.mongodb.MDStoreCollectorPlugin;
import eu.dnetlib.dhp.collection.plugin.mongodb.MongoDbDumpCollectorPlugin;
import eu.dnetlib.dhp.collection.plugin.oai.OaiCollectorPlugin;
import eu.dnetlib.dhp.collection.plugin.osf.OsfPreprintsCollectorPlugin;
import eu.dnetlib.dhp.collection.plugin.rest.RestCollectorPlugin;
import eu.dnetlib.dhp.common.aggregation.AggregatorReport;
import eu.dnetlib.dhp.common.collection.CollectorException;
import eu.dnetlib.dhp.common.collection.HttpClientParams;
import eu.dnetlib.dhp.schema.mdstore.MDStoreVersion;

public class CollectorWorker extends ReportingJob {

	private static final Logger log = LoggerFactory.getLogger(CollectorWorker.class);

	private final ApiDescriptor api;

	private final FileSystem fileSystem;

	private final MDStoreVersion mdStoreVersion;

	private final HttpClientParams clientParams;

	public CollectorWorker(
			final ApiDescriptor api,
			final FileSystem fileSystem,
			final MDStoreVersion mdStoreVersion,
			final HttpClientParams clientParams,
			final AggregatorReport report) {
		super(report);
		this.api = api;
		this.fileSystem = fileSystem;
		this.mdStoreVersion = mdStoreVersion;
		this.clientParams = clientParams;
	}

	public void collect() throws UnknownCollectorPluginException, CollectorException, IOException {

		final String outputPath = this.mdStoreVersion.getHdfsPath() + SEQUENCE_FILE_NAME;
		log.info("outputPath path is {}", outputPath);

		final CollectorPlugin plugin = getCollectorPlugin();
		final AtomicInteger counter = new AtomicInteger(0);

		scheduleReport(counter);

		try (SequenceFile.Writer writer = SequenceFile
				.createWriter(this.fileSystem.getConf(), SequenceFile.Writer.file(new Path(outputPath)), SequenceFile.Writer
						.keyClass(IntWritable.class), SequenceFile.Writer
								.valueClass(Text.class), SequenceFile.Writer.compression(SequenceFile.CompressionType.BLOCK, new DeflateCodec()))) {
			final IntWritable key = new IntWritable(counter.get());
			final Text value = new Text();
			plugin
					.collect(this.api, this.report)
					.forEach(content -> {
						key.set(counter.getAndIncrement());
						value.set(content);
						try {
							writer.append(key, value);
						} catch (final Throwable e) {
							throw new RuntimeException(e);
						}
					});
		} catch (final Throwable e) {
			this.report.put(e.getClass().getName(), e.getMessage());
			throw new CollectorException(e);
		} finally {
			shutdown();
			this.report.ongoing(counter.longValue(), counter.longValue());
		}
	}

	private void scheduleReport(final AtomicInteger counter) {
		schedule(new ReporterCallback() {

			@Override
			public Long getCurrent() {
				return counter.longValue();
			}

			@Override
			public Long getTotal() {
				return null;
			}
		});
	}

	private CollectorPlugin getCollectorPlugin() throws UnknownCollectorPluginException {

		switch (CollectorPlugin.NAME.valueOf(this.api.getProtocol())) {
		case oai:
			return new OaiCollectorPlugin(this.clientParams);
		case rest_json2xml:
			return new RestCollectorPlugin(this.clientParams);
		case file:
			return new FileCollectorPlugin(this.fileSystem);
		case fileGzip:
			return new FileGZipCollectorPlugin(this.fileSystem);
		case baseDump:
			return new BaseCollectorPlugin(this.fileSystem);
		case gtr2Publications:
			return new Gtr2PublicationsCollectorPlugin(this.clientParams);
		case osfPreprints:
			return new OsfPreprintsCollectorPlugin(this.clientParams);
		case other:
			final CollectorPlugin.NAME.OTHER_NAME plugin = Optional
					.ofNullable(this.api.getParams().get("other_plugin_type"))
					.map(CollectorPlugin.NAME.OTHER_NAME::valueOf)
					.orElseThrow(() -> new IllegalArgumentException("invalid other_plugin_type"));

			switch (plugin) {
			case mdstore_mongodb_dump:
				return new MongoDbDumpCollectorPlugin(this.fileSystem);
			case mdstore_mongodb:
				return new MDStoreCollectorPlugin();
			default:
				throw new UnknownCollectorPluginException("plugin is not managed: " + plugin);
			}
		default:
			throw new UnknownCollectorPluginException("protocol is not managed: " + this.api.getProtocol());
		}
	}

}
