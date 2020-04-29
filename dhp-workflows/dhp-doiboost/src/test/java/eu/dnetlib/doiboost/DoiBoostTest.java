package eu.dnetlib.doiboost;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import eu.dnetlib.doiboost.crossref.SparkMapDumpIntoOAF;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DoiBoostTest {

  final ObjectMapper mapper = new ObjectMapper().enable(SerializationFeature.INDENT_OUTPUT);
  Logger logger = LoggerFactory.getLogger(DoiBoostTest.class);

  @Test
  public void test() throws Exception {

    // SparkDownloadContentFromCrossref.main(null);
    // CrossrefImporter.main("-n file:///tmp -t file:///tmp/p.seq -ts 1586110000749".split("
    // "));
    SparkMapDumpIntoOAF.main(
        "-m local[*] -s file:///data/doiboost/crossref_dump.seq -t /data/doiboost".split(" "));
  }
}
