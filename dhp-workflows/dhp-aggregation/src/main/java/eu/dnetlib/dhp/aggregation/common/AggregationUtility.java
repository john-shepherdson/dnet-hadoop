
package eu.dnetlib.dhp.aggregation.common;

import java.io.BufferedOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;

import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.spark.sql.SparkSession;

public class AggregationUtility {

	public static void writeTotalSizeOnHDFS(final SparkSession spark, final Long total, final String path)
		throws IOException {

		FileSystem fs = FileSystem.get(spark.sparkContext().hadoopConfiguration());

		FSDataOutputStream output = fs.create(new Path(path));

		final BufferedOutputStream os = new BufferedOutputStream(output);

		os.write(total.toString().getBytes(StandardCharsets.UTF_8));

		os.close();
	}
}
