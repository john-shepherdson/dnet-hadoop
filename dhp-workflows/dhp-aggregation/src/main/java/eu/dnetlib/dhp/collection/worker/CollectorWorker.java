
package eu.dnetlib.dhp.collection.worker;

import java.io.IOException;
import java.net.URI;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.databind.ObjectMapper;

import eu.dnetlib.collector.worker.model.ApiDescriptor;
import eu.dnetlib.dhp.application.ArgumentApplicationParser;
import eu.dnetlib.dhp.collection.plugin.CollectorPlugin;
import eu.dnetlib.dhp.collection.worker.utils.CollectorPluginFactory;

public class CollectorWorker {

	private static final Logger log = LoggerFactory.getLogger(CollectorWorker.class);

	private final CollectorPluginFactory collectorPluginFactory;

	private final ApiDescriptor api;

	private final String hdfsuri;

	private final String hdfsPath;

	public CollectorWorker(
		final CollectorPluginFactory collectorPluginFactory,
		final ApiDescriptor api,
		final String hdfsuri,
		final String hdfsPath) {
		this.collectorPluginFactory = collectorPluginFactory;
		this.api = api;
		this.hdfsuri = hdfsuri;
		this.hdfsPath = hdfsPath;

	}

	public void collect() throws CollectorException {
		try {
			final CollectorPlugin plugin = collectorPluginFactory.getPluginByProtocol(api.getProtocol());

			// ====== Init HDFS File System Object
			Configuration conf = new Configuration();
			// Set FileSystem URI
			conf.set("fs.defaultFS", hdfsuri);
			// Because of Maven
			conf.set("fs.hdfs.impl", org.apache.hadoop.hdfs.DistributedFileSystem.class.getName());
			conf.set("fs.file.impl", org.apache.hadoop.fs.LocalFileSystem.class.getName());

			System.setProperty("hadoop.home.dir", "/");
			// Get the filesystem - HDFS
			FileSystem.get(URI.create(hdfsuri), conf);
			Path hdfswritepath = new Path(hdfsPath);

			log.info("Created path " + hdfswritepath.toString());

			final AtomicInteger counter = new AtomicInteger(0);
			try (SequenceFile.Writer writer = SequenceFile
				.createWriter(
					conf,
					SequenceFile.Writer.file(hdfswritepath),
					SequenceFile.Writer.keyClass(IntWritable.class),
					SequenceFile.Writer.valueClass(Text.class))) {
				final IntWritable key = new IntWritable(counter.get());
				final Text value = new Text();
				plugin
					.collect(api)
					.forEach(
						content -> {
							key.set(counter.getAndIncrement());
							value.set(content);
							try {
								writer.append(key, value);
							} catch (IOException e) {
								throw new RuntimeException(e);
							}
						});
			}
		} catch (Throwable e) {
			throw new CollectorException("Error on collecting ", e);
		}
	}
}
