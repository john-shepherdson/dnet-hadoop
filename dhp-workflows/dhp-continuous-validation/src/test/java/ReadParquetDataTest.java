import java.util.Map;

import org.junit.jupiter.api.Test;
import org.slf4j.LoggerFactory;

import eu.dnetlib.dhp.continuous_validator.utils.ParquetUtils;
import eu.dnetlib.validator2.validation.utils.TestUtils;

public class ReadParquetDataTest {

	private static final org.slf4j.Logger logger = LoggerFactory.getLogger(ReadParquetDataTest.class);

	private static final String parquetFileFullPath = TestUtils.TEST_FILES_BASE_DIR
		+ "part-00589-733117df-3822-4fce-bded-17289cc5959a-c000.snappy.parquet";

	public static void main(String[] args) {

		testParquetRead();
	}

	@Test
	public static void testParquetRead() {
		Map<String, String> idXmlMap = ParquetUtils.getIdXmlMapFromParquetFile(parquetFileFullPath);
		if (idXmlMap == null) {
			logger.error("Could not create the \"idXmlMap\" from parquet-file: " + parquetFileFullPath);
			System.exit(99);
		} else if (idXmlMap.isEmpty())
			logger.warn("The generated \"idXmlMap\" was empty, for parquet-file: " + parquetFileFullPath);
		else
			logger.info("The \"idXmlMap\" was successfully generated, for parquet-file: " + parquetFileFullPath);
	}

}
