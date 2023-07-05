
package eu.dnetlib.dhp.actionmanager;

import java.util.Optional;

import eu.dnetlib.dhp.common.HdfsSupport;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.SparkSession;

import com.fasterxml.jackson.databind.ObjectMapper;

import eu.dnetlib.dhp.application.ArgumentApplicationParser;
import eu.dnetlib.dhp.schema.common.ModelConstants;
import eu.dnetlib.dhp.schema.oaf.StructuredProperty;
import eu.dnetlib.dhp.schema.oaf.Subject;
import eu.dnetlib.dhp.schema.oaf.utils.OafMapperUtils;

public class Constants {

	public static final String DOI = "doi";
	public static final String DOI_CLASSNAME = "Digital Object Identifier";

	public static final String DEFAULT_DELIMITER = ",";
	public static final String DEFAULT_FOS_DELIMITER = "\t";

	public static final String UPDATE_DATA_INFO_TYPE = "update";
	public static final String UPDATE_SUBJECT_FOS_CLASS_ID = "subject:fos";
	public static final String UPDATE_CLASS_NAME = "Inferred by OpenAIRE";
	public static final String UPDATE_MEASURE_BIP_CLASS_ID = "measure:bip";
	public static final String UPDATE_SUBJECT_SDG_CLASS_ID = "subject:sdg";
	public static final String UPDATE_MEASURE_USAGE_COUNTS_CLASS_ID = "measure:usage_counts";
	public static final String UPDATE_KEY_USAGE_COUNTS = "count";

	public static final String FOS_CLASS_ID = "FOS";
	public static final String FOS_CLASS_NAME = "Fields of Science and Technology classification";

	public static final String SDG_CLASS_ID = "SDG";
	public static final String SDG_CLASS_NAME = "Sustainable Development Goals";

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

	public static Subject getSubject(String sbj, String classid, String classname,
		String diqualifierclassid) {
		if (sbj == null || sbj.equals(NULL))
			return null;
		Subject s = new Subject();
		s.setValue(sbj);
		s
			.setQualifier(
				OafMapperUtils
					.qualifier(
						classid,
						classname,
						ModelConstants.DNET_SUBJECT_TYPOLOGIES,
						ModelConstants.DNET_SUBJECT_TYPOLOGIES));
		s
			.setDataInfo(
				OafMapperUtils
					.dataInfo(
						false,
						UPDATE_DATA_INFO_TYPE,
						true,
						false,
						OafMapperUtils
							.qualifier(
								diqualifierclassid,
								UPDATE_CLASS_NAME,
								ModelConstants.DNET_PROVENANCE_ACTIONS,
								ModelConstants.DNET_PROVENANCE_ACTIONS),
						""));

		return s;

	}
	public static void removeOutputDir(SparkSession spark, String path) {
		HdfsSupport.remove(path, spark.sparkContext().hadoopConfiguration());
	}

}
