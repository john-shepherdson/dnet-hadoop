
package eu.dnetlib.dhp.actionmanager.common;

import java.util.Optional;

import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.SparkSession;

import com.fasterxml.jackson.databind.ObjectMapper;

import eu.dnetlib.dhp.application.ArgumentApplicationParser;

public class Constants {

	public static final String DOI = "doi";

	public static final String UPDATE_DATA_INFO_TYPE = "update";
	public static final String UPDATE_SUBJECT_FOS_CLASS_ID = "subject:fos";
	public static final String UPDATE_CLASS_NAME = "Inferred by OpenAIRE";
	public static final String UPDATE_MEASURE_BIP_CLASS_ID = "measure:bip";

	public static final String FOS_CLASS_ID = "FOS";
	public static final String FOS_CLASS_NAME = "Fields of Science and Technology classification";

	public static final String NULL = "NULL";

	public static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

	private Constants() {
	}

	public static Boolean isSparkSessionManaged(ArgumentApplicationParser parser) {
		return Optional
			.ofNullable(parser.get("isSparkSessionManaged"))
			.map(Boolean::valueOf)
			.orElse(Boolean.TRUE);
	}

	public static <R> Dataset<R> readPath(
		SparkSession spark, String inputPath, Class<R> clazz) {
		return spark
			.read()
			.textFile(inputPath)
			.map((MapFunction<String, R>) value -> OBJECT_MAPPER.readValue(value, clazz), Encoders.bean(clazz));
	}

}
