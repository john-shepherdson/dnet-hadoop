
package eu.dnetlib.dhp.collection.worker;

import static eu.dnetlib.dhp.common.Constants.SEQUENCE_FILE_NAME;

import java.io.IOException;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import eu.dnetlib.data.mdstore.manager.common.model.MDStoreVersion;
import eu.dnetlib.dhp.collection.ApiDescriptor;
import eu.dnetlib.dhp.collection.plugin.CollectorPlugin;
import eu.dnetlib.dhp.message.MessageSender;

public class CollectorWorker {

	private static final Logger log = LoggerFactory.getLogger(CollectorWorker.class);

	private final ApiDescriptor api;

	private final Configuration conf;

	private final MDStoreVersion mdStoreVersion;

	private final HttpClientParams clientParams;

	private final CollectorPluginReport report;

	private final MessageSender messageSender;

	public CollectorWorker(
		final ApiDescriptor api,
		final Configuration conf,
		final MDStoreVersion mdStoreVersion,
		final HttpClientParams clientParams,
		final MessageSender messageSender,
		final CollectorPluginReport report) {
		this.api = api;
		this.conf = conf;
		this.mdStoreVersion = mdStoreVersion;
		this.clientParams = clientParams;
		this.messageSender = messageSender;
		this.report = report;
	}

	public void collect() throws UnknownCollectorPluginException, CollectorException, IOException {

		final String outputPath = mdStoreVersion.getHdfsPath() + SEQUENCE_FILE_NAME;
		log.info("outputPath path is {}", outputPath);

		final CollectorPlugin plugin = CollectorPluginFactory.getPluginByProtocol(clientParams, api.getProtocol());
		final AtomicInteger counter = new AtomicInteger(0);

		try (SequenceFile.Writer writer = SequenceFile
			.createWriter(
				conf,
				SequenceFile.Writer.file(new Path(outputPath)),
				SequenceFile.Writer.keyClass(IntWritable.class),
				SequenceFile.Writer.valueClass(Text.class))) {
			final IntWritable key = new IntWritable(counter.get());
			final Text value = new Text();
			plugin
				.collect(api, report)
				.forEach(
					content -> {
						key.set(counter.getAndIncrement());
						if (counter.get() % 500 == 0)
							messageSender.sendMessage(counter.longValue(), null);
						value.set(content);
						try {
							writer.append(key, value);
						} catch (Throwable e) {
							report.put(e.getClass().getName(), e.getMessage());
							log.warn("setting report to failed");
							report.setSuccess(false);
							throw new RuntimeException(e);
						}
					});
		} catch (Throwable e) {
			report.put(e.getClass().getName(), e.getMessage());
			log.warn("setting report to failed");
			report.setSuccess(false);
		} finally {
			messageSender.sendMessage(counter.longValue(), counter.longValue());
		}
	}

}
