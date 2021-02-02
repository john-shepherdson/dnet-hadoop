
package eu.dnetlib.dhp.aggregation.common;

import java.io.BufferedOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;

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

	public static void writeTotalSizeOnHDFS(final SparkSession spark, final Long total, final String path)
		throws IOException {

		log.info("writing size ({}) info file {}", total, path);
		try (FileSystem fs = FileSystem.get(spark.sparkContext().hadoopConfiguration());
			BufferedOutputStream os = new BufferedOutputStream(fs.create(new Path(path)))) {
			os.write(total.toString().getBytes(StandardCharsets.UTF_8));
			os.flush();
		}

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
