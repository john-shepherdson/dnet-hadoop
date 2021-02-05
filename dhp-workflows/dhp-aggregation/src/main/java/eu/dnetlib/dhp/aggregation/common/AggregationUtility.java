
package eu.dnetlib.dhp.aggregation.common;

import java.io.BufferedOutputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;

import org.apache.commons.io.IOUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.databind.ObjectMapper;

import eu.dnetlib.dhp.collection.GenerateNativeStoreSparkJob;
import eu.dnetlib.dhp.model.mdstore.MetadataRecord;

public class AggregationUtility {

	private static final Logger log = LoggerFactory.getLogger(AggregationUtility.class);

	public static final ObjectMapper MAPPER = new ObjectMapper();

	public static void writeHdfsFile(final Configuration conf, final String content, final String path)
		throws IOException {

		log.info("writing file {}, size {}", path, content.length());
		try (FileSystem fs = FileSystem.get(conf);
			BufferedOutputStream os = new BufferedOutputStream(fs.create(new Path(path)))) {
			os.write(content.getBytes(StandardCharsets.UTF_8));
			os.flush();
		}
	}

	public static String readHdfsFile(Configuration conf, String path) throws IOException {
		log.info("reading file {}", path);

		try (FileSystem fs = FileSystem.get(conf)) {
			final Path p = new Path(path);
			if (!fs.exists(p)) {
				throw new FileNotFoundException(path);
			}
			return IOUtils.toString(fs.open(p));
		}
	}

	public static <T> T readHdfsFileAs(Configuration conf, String path, Class<T> clazz) throws IOException {
		return MAPPER.readValue(readHdfsFile(conf, path), clazz);
	}

	public static <T> void saveDataset(final Dataset<T> mdstore, final String targetPath) {
		log.info("saving dataset in: {}", targetPath);
		mdstore
			.write()
			.mode(SaveMode.Overwrite)
			.format("parquet")
			.save(targetPath);
	}

}
