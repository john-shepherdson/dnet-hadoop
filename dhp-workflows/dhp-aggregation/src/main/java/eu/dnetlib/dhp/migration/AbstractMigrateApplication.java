package eu.dnetlib.dhp.migration;

import java.io.Closeable;
import java.io.IOException;
import java.net.URI;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.codehaus.jackson.map.ObjectMapper;

import eu.dnetlib.dhp.schema.oaf.Oaf;

public class AbstractMigrateApplication implements Closeable {

	private final AtomicInteger counter = new AtomicInteger(0);

	private final IntWritable key = new IntWritable(counter.get());

	private final Text value = new Text();

	private final ObjectMapper objectMapper = new ObjectMapper();

	private final SequenceFile.Writer writer;

	public AbstractMigrateApplication(final String hdfsPath, final String hdfsNameNode, final String hdfsUser) throws Exception {
		this.writer = SequenceFile.createWriter(getConf(hdfsNameNode, hdfsUser), SequenceFile.Writer.file(new Path(hdfsPath)), SequenceFile.Writer
				.keyClass(IntWritable.class), SequenceFile.Writer.valueClass(Text.class));
	}

	private Configuration getConf(final String hdfsNameNode, final String hdfsUser) throws IOException {
		final Configuration conf = new Configuration();
		conf.set("fs.defaultFS", hdfsNameNode);
		conf.set("fs.hdfs.impl", org.apache.hadoop.hdfs.DistributedFileSystem.class.getName());
		conf.set("fs.file.impl", org.apache.hadoop.fs.LocalFileSystem.class.getName());
		System.setProperty("HADOOP_USER_NAME", hdfsUser);
		System.setProperty("hadoop.home.dir", "/");
		FileSystem.get(URI.create(hdfsNameNode), conf);
		return conf;
	}

	protected void emitOaf(final Oaf oaf) {
		try {
			key.set(counter.getAndIncrement());
			value.set(objectMapper.writeValueAsString(oaf));
			writer.append(key, value);
		} catch (final Exception e) {
			e.printStackTrace();
		}
	}

	@Override
	public void close() throws IOException {
		writer.close();
	}

}
