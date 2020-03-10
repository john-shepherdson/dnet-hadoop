package eu.dnetlib.dhp.migration.utils;

import java.io.Closeable;
import java.io.IOException;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.codehaus.jackson.map.ObjectMapper;

import eu.dnetlib.dhp.schema.oaf.Oaf;

public class AbstractMigrationApplication implements Closeable {

	private final AtomicInteger counter = new AtomicInteger(0);

	private final Text key = new Text();

	private final Text value = new Text();

	private final SequenceFile.Writer writer;

	private final ObjectMapper objectMapper = new ObjectMapper();

	private static final Log log = LogFactory.getLog(AbstractMigrationApplication.class);

	public AbstractMigrationApplication(final String hdfsPath) throws Exception {

		log.info(String.format("Creating SequenceFile Writer, hdfsPath=%s", hdfsPath));

		this.writer = SequenceFile.createWriter(getConf(), SequenceFile.Writer.file(new Path(hdfsPath)), SequenceFile.Writer
				.keyClass(Text.class), SequenceFile.Writer.valueClass(Text.class));
	}

	private Configuration getConf() throws IOException {
		final Configuration conf = new Configuration();
		/*
		 * conf.set("fs.defaultFS", hdfsNameNode); conf.set("fs.hdfs.impl", org.apache.hadoop.hdfs.DistributedFileSystem.class.getName());
		 * conf.set("fs.file.impl", org.apache.hadoop.fs.LocalFileSystem.class.getName()); System.setProperty("HADOOP_USER_NAME", hdfsUser);
		 * System.setProperty("hadoop.home.dir", "/"); FileSystem.get(URI.create(hdfsNameNode), conf);
		 */
		return conf;
	}

	protected void emit(final String s, final String type) {
		try {
			key.set(counter.getAndIncrement() + ":" + type);
			value.set(s);
			writer.append(key, value);
		} catch (final Exception e) {
			throw new RuntimeException(e);
		}
	}

	protected void emitOaf(final Oaf oaf) {
		try {
			emit(objectMapper.writeValueAsString(oaf), oaf.getClass().getSimpleName().toLowerCase());
		} catch (final Exception e) {
			throw new RuntimeException(e);
		}
	}

	public ObjectMapper getObjectMapper() {
		return objectMapper;
	}

	@Override
	public void close() throws IOException {
		writer.hflush();
		writer.close();
	}

}
