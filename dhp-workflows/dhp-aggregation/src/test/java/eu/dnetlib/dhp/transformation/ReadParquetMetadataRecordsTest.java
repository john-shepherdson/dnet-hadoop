
package eu.dnetlib.dhp.transformation;

import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.util.Arrays;

import org.apache.commons.lang3.ArrayUtils;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.function.ForeachFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.functions;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.StructField;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

import eu.dnetlib.dhp.collection.GenerateNativeStoreSparkJob;
import eu.dnetlib.dhp.schema.mdstore.MetadataRecord;
import eu.dnetlib.enabling.is.lookup.rmi.ISLookUpException;

public class ReadParquetMetadataRecordsTest {

	private static final String VALIDATED_PARQUET_ROOT_DIR = "/Users/michele/Develop/temp/store_native_validated";
	private static final String NOT_VALIDATED_PARQUET_ROOT_DIR = "/Users/michele/Develop/temp/store_native_not_validated";

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
	void testReadParquetAddNewField() throws IOException {
		try (final SparkSession spark = SparkSession.builder().config(this.sparkConf).getOrCreate()) {

			final Dataset<MetadataRecord> records1 = spark
				.read()
				.parquet(VALIDATED_PARQUET_ROOT_DIR)
				.as(Encoders.bean(MetadataRecord.class));

			final DataType dataType = Arrays
				.stream(
					records1
						.schema()
						.fields())
				.filter(f -> GenerateNativeStoreSparkJob.VALIDATION_RESULTS_FIELD.equals(f.name()))
				.map(StructField::dataType)
				.findFirst()
				.orElseThrow(
					() -> new RuntimeException(
						"Missing " + GenerateNativeStoreSparkJob.VALIDATION_RESULTS_FIELD + " field in new schema"));
			final Dataset<Row> rows2 = spark.read().parquet(NOT_VALIDATED_PARQUET_ROOT_DIR);

			// FIX
			final Dataset<Row> rowsWithNewField = ArrayUtils
				.contains(rows2.schema().fieldNames(), GenerateNativeStoreSparkJob.VALIDATION_RESULTS_FIELD) ? rows2
					: rows2
						.withColumn(
							GenerateNativeStoreSparkJob.VALIDATION_RESULTS_FIELD, functions.lit(null).cast(dataType));

			final Dataset<MetadataRecord> records2 = rowsWithNewField.as(Encoders.bean(MetadataRecord.class));
			// END FIX

			final Dataset<MetadataRecord> records = records1.union(records2);

			records.foreach((ForeachFunction<MetadataRecord>) r -> System.out.println(r.getId()));

			assertTrue(records.count() > 0);
		}
	}

	@Test
	@Disabled
	void testReadParquet() throws IOException {
		try (final SparkSession spark = SparkSession.builder().config(this.sparkConf).getOrCreate()) {

			final Dataset<MetadataRecord> records = spark
				.read()
				.parquet(VALIDATED_PARQUET_ROOT_DIR)
				.as(Encoders.bean(MetadataRecord.class));

			records.foreach((ForeachFunction<MetadataRecord>) r -> System.out.println(r.getId()));

			assertTrue(records.count() > 0);
		}
	}

}
