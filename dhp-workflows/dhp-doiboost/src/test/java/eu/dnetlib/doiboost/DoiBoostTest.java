package eu.dnetlib.doiboost;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.jayway.jsonpath.JsonPath;
import eu.dnetlib.dhp.schema.oaf.Result;
import eu.dnetlib.doiboost.crossref.Crossref2Oaf;
import eu.dnetlib.doiboost.crossref.SparkMapDumpIntoOAF;
import org.apache.commons.io.IOUtils;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.platform.commons.util.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


import java.io.IOException;
import java.util.List;

public class DoiBoostTest {

    Logger logger = LoggerFactory.getLogger(DoiBoostTest.class);

    @Test
    public void test() throws Exception {

        //SparkDownloadContentFromCrossref.main(null);
        //CrossrefImporter.main("-n file:///tmp -t file:///tmp/p.seq -ts 1586110000749".split(" "));
        SparkMapDumpIntoOAF.main("-m local[*] -s file:///data/doiboost/crossref_dump.seq".split(" "));
    }



    @Test
    public void testConvertCrossRef2Oaf() throws IOException {

        final String json = IOUtils.toString(getClass().getResourceAsStream("pc.json"));
        ObjectMapper mapper = new ObjectMapper().enable(SerializationFeature.INDENT_OUTPUT);
        Assertions.assertNotNull(json);
        Assertions.assertFalse(StringUtils.isBlank(json));



        Crossref2Oaf cf = new Crossref2Oaf();
        final Result result = cf.convert(json, logger);
        Assertions.assertNotNull(result);

        logger.info(mapper.writeValueAsString(result));

    }





    @Test
    public void testPath() throws Exception {
        final String json = IOUtils.toString(getClass().getResourceAsStream("response.json"));
        final List<String > res = JsonPath.read(json, "$.hits.hits[*]._source.blob");
        System.out.println(res.size());

    }





}
