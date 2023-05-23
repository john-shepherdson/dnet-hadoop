
package eu.dnetlib.dhp.oa.graph.raw.common;

import java.io.Closeable;
import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

import org.apache.commons.lang3.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SparkSession;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

import eu.dnetlib.dhp.common.HdfsSupport;
import eu.dnetlib.dhp.schema.oaf.Oaf;
import eu.dnetlib.dhp.utils.DHPUtils;

public class AbstractMigrationApplication implements Closeable {

	private final AtomicInteger counter = new AtomicInteger(0);

	private final Text key = new Text();

	private final Text value = new Text();

	private final SequenceFile.Writer writer;

	private final ObjectMapper objectMapper = new ObjectMapper();

	private static final Log log = LogFactory.getLog(AbstractMigrationApplication.class);

	protected AbstractMigrationApplication() { // ONLY FOR UNIT TEST
		this.writer = null;
	}

	public AbstractMigrationApplication(final String hdfsPath) throws IOException {

		log.info(String.format("Creating SequenceFile Writer, hdfsPath=%s", hdfsPath));

		this.writer = SequenceFile
			.createWriter(
				getConf(),
				SequenceFile.Writer.file(new Path(hdfsPath)),
				SequenceFile.Writer.keyClass(Text.class),
				SequenceFile.Writer.valueClass(Text.class));
	}

	/**
	 * Retrieves from the metadata store manager application the list of paths associated with mdstores characterized
	 * by he given format, layout, interpretation
	 * @param mdstoreManagerUrl the URL of the mdstore manager service
	 * @param format the mdstore format
	 * @param layout the mdstore layout
	 * @param interpretation the mdstore interpretation
	 * @return the set of hdfs paths
	 * @throws IOException in case of HTTP communication issues
	 */
	protected static Set<String> mdstorePaths(final String mdstoreManagerUrl,
		final String format,
		final String layout,
		final String interpretation) throws IOException {
		return DHPUtils.mdstorePaths(mdstoreManagerUrl, format, layout, interpretation, false);
	}

	private Configuration getConf() {
		return new Configuration();
		/*
		 * conf.set("fs.defaultFS", hdfsNameNode); conf.set("fs.hdfs.impl",
		 * org.apache.hadoop.hdfs.DistributedFileSystem.class.getName()); conf.set("fs.file.impl",
		 * org.apache.hadoop.fs.LocalFileSystem.class.getName()); System.setProperty("HADOOP_USER_NAME", hdfsUser);
		 * System.setProperty("hadoop.home.dir", "/"); FileSystem.get(URI.create(hdfsNameNode), conf);
		 */
	}

	protected void emit(final String s, final String type) {
		try {
			key.set(counter.getAndIncrement() + ":" + type);
			value.set(s);
			writer.append(key, value);
		} catch (final IOException e) {
			throw new IllegalStateException(e);
		}
	}

	protected void emitOaf(final Oaf oaf) {
		try {
			emit(objectMapper.writeValueAsString(oaf), oaf.getClass().getSimpleName().toLowerCase());
		} catch (JsonProcessingException e) {
			throw new IllegalStateException(e);
		}
	}

	protected static List<String> listEntityPaths(final SparkSession spark, final String paths) {
		final JavaSparkContext sc = JavaSparkContext.fromSparkContext(spark.sparkContext());
		return Arrays
			.stream(paths.split(","))
			.filter(StringUtils::isNotBlank)
			.filter(p -> HdfsSupport.exists(p, sc.hadoopConfiguration()) || p.contains("/*"))
			.collect(Collectors.toList());
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
