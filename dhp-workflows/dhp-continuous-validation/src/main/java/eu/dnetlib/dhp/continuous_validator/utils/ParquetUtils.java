
package eu.dnetlib.dhp.continuous_validator.utils;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.avro.generic.GenericRecord;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.avro.AvroParquetReader;
import org.apache.parquet.hadoop.ParquetReader;
import org.apache.parquet.hadoop.util.HadoopInputFile;
import org.apache.parquet.io.InputFile;
import org.slf4j.LoggerFactory;

public class ParquetUtils {

	private static final org.slf4j.Logger logger = LoggerFactory.getLogger(ParquetUtils.class);

	private static final Configuration parquetConfig = new Configuration();

	public static List<GenericRecord> getParquetRecords(String fullFilePath) {
		InputFile inputFile;
		try { // TODO - Verify that this will create any directories which do not exist in the provided path. Currently
				// we create the directories beforehand.
			inputFile = HadoopInputFile.fromPath(new Path(fullFilePath), parquetConfig);
			// logger.trace("Created the parquet " + outputFile); // DEBUG!
		} catch (Throwable e) { // The simple "Exception" may not be thrown here, but an "Error" may be thrown.
								// "Throwable" catches EVERYTHING!
			logger.error("", e);
			return null;
		}

		List<GenericRecord> records = new ArrayList<>();
		GenericRecord record;
		try (ParquetReader<GenericRecord> reader = AvroParquetReader.<GenericRecord> builder(inputFile).build()) {
			while ((record = reader.read()) != null) {
				records.add(record);
			}
		} catch (Throwable e) { // The simple "Exception" may not be thrown here, but an "Error" may be thrown.
								// "Throwable" catches EVERYTHING!
			logger.error("Problem when creating the \"ParquetWriter\" object or when writing the records with it!", e);

			// At some point, I got an "NoSuchMethodError", because of a problem in the AvroSchema file:
			// (java.lang.NoSuchMethodError: org.apache.avro.Schema.getLogicalType()Lorg/apache/avro/LogicalType;).
			// The error was with the schema: {"name": "date", "type" : ["null", {"type" : "long", "logicalType" :
			// "timestamp-millis"}]},
			return null;
		}

		return records; // It may be empty.
	}

	public static Map<String, String> getIdXmlMapFromParquetFile(String parquetFileFullPath) {
		List<GenericRecord> recordList = ParquetUtils.getParquetRecords(parquetFileFullPath);
		if (recordList == null)
			return null; // The error is already logged.
		else if (recordList.isEmpty()) {
			logger.error("The parquet-file \"" + parquetFileFullPath + "\" had no records inside!");
			return null;
		}

		Map<String, String> idXmlMap = new HashMap<>();

		for (GenericRecord record : recordList) {
			if (logger.isTraceEnabled())
				logger.trace(record.toString());

			Object id = record.get("id");
			if (id == null)
				continue;
			String idStr = id.toString();

			Object encoding = record.get("encoding");
			if (encoding == null) {
				logger.warn("Record with id = \"" + idStr + "\" does not provide the encoding for its body!");
				continue;
			}
			String encodingStr = encoding.toString();
			if (!encodingStr.equals("XML")) {
				logger.warn("Record with id = \"" + idStr + "\" does not have XML encoding for its body!");
				continue;
			}

			Object body = record.get("body");
			if (body == null) {
				logger.warn("Record with id = \"" + idStr + "\" does not have a body!");
				continue;
			}
			String bodyStr = body.toString();

			idXmlMap.put(idStr, bodyStr);
			// logger.debug(idStr + " | " + idXmlMap.get(idStr));
		}

		return idXmlMap;
	}

}
