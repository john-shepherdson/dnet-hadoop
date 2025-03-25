
package eu.dnetlib.dhp.transformation;

import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;

import org.apache.commons.lang3.ArrayUtils;
import org.apache.spark.SparkConf;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.functions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

import eu.dnetlib.dhp.schema.mdstore.MetadataRecord;
import eu.dnetlib.enabling.is.lookup.rmi.ISLookUpException;

public class ReadParquetMetadataRecordsTest {

	private static final String PARQUET_ROOT_DIR = "/Users/michele/Develop/temp/store_native_validated";

	private SparkConf sparkConf;

	@BeforeEach
	public void setUp() throws IOException, ISLookUpException {
		this.sparkConf = new SparkConf();
		this.sparkConf.setMaster("local[*]");
		this.sparkConf.set("spark.driver.host", "localhost");
		this.sparkConf.set("spark.ui.enabled", "false");
	}

	@Test
	@Disabled
	void testReadParquetAddNewField() {
		try (final SparkSession spark = SparkSession.builder().config(this.sparkConf).getOrCreate()) {
			final Dataset<Row> rows = spark.read().parquet(PARQUET_ROOT_DIR);

			final Dataset<Row> rowsWithNewField = ArrayUtils.contains(rows.schema().fieldNames(), "testField") ? rows
					: rows.withColumn("testField", functions.map());

			final Dataset<TestMetadataRecord> records = rowsWithNewField.as(Encoders.bean(TestMetadataRecord.class));

			records.foreach(r -> System.out.println(r.getId() + " -- " + r.getTestField()));

			assertTrue(records.count() > 0);
		}
	}

	@Test
	@Disabled
	void testReadParquet() {
		try (final SparkSession spark = SparkSession.builder().config(this.sparkConf).getOrCreate()) {
			final Dataset<Row> rows = spark.read().parquet(PARQUET_ROOT_DIR);

			final Dataset<MetadataRecord> records = rows.as(Encoders.bean(MetadataRecord.class));

			records.foreach(r -> System.out.println(r.getId()));

			assertTrue(records.count() > 0);
		}
	}

}
