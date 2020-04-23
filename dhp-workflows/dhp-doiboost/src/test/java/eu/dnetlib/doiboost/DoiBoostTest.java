package eu.dnetlib.doiboost;

import static org.junit.jupiter.api.Assertions.*;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.jayway.jsonpath.JsonPath;
import eu.dnetlib.dhp.schema.oaf.Result;
import eu.dnetlib.doiboost.crossref.Crossref2Oaf;
import eu.dnetlib.doiboost.crossref.SparkMapDumpIntoOAF;
import java.io.IOException;
import java.util.List;
import org.apache.commons.io.IOUtils;
import org.junit.jupiter.api.Test;
import org.junit.platform.commons.util.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DoiBoostTest {

    Logger logger = LoggerFactory.getLogger(DoiBoostTest.class);

    public void test() throws Exception {

        // SparkDownloadContentFromCrossref.main(null);
        // CrossrefImporter.main("-n file:///tmp -t file:///tmp/p.seq -ts 1586110000749".split("
        // "));
        SparkMapDumpIntoOAF.main(
                "-m local[*] -s file:///data/doiboost/crossref_dump.seq -t /data/doiboost"
                        .split(" "));
    }

    @Test
    public void testConvertDatasetCrossRef2Oaf() throws IOException {
        final String json = IOUtils.toString(getClass().getResourceAsStream("dataset.json"));
        ObjectMapper mapper = new ObjectMapper().enable(SerializationFeature.INDENT_OUTPUT);
        assertNotNull(json);
        assertFalse(StringUtils.isBlank(json));
        final Result result = Crossref2Oaf.convert(json, logger);

        logger.info(mapper.writeValueAsString(result));
    }

    @Test
    public void testConvertPreprintCrossRef2Oaf() throws IOException {

        final String json = IOUtils.toString(getClass().getResourceAsStream("preprint.json"));
        ObjectMapper mapper = new ObjectMapper().enable(SerializationFeature.INDENT_OUTPUT);
        assertNotNull(json);
        assertFalse(StringUtils.isBlank(json));

        final Result result = Crossref2Oaf.convert(json, logger);
        assertNotNull(result);

        assertNotNull(result.getDataInfo(), "Datainfo test not null Failed");
        assertNotNull(
                result.getDataInfo().getProvenanceaction(),
                "DataInfo/Provenance test not null Failed");
        assertFalse(
                StringUtils.isBlank(result.getDataInfo().getProvenanceaction().getClassid()),
                "DataInfo/Provenance/classId test not null Failed");
        assertFalse(
                StringUtils.isBlank(result.getDataInfo().getProvenanceaction().getClassname()),
                "DataInfo/Provenance/className test not null Failed");
        assertFalse(
                StringUtils.isBlank(result.getDataInfo().getProvenanceaction().getSchemeid()),
                "DataInfo/Provenance/SchemeId test not null Failed");
        assertFalse(
                StringUtils.isBlank(result.getDataInfo().getProvenanceaction().getSchemename()),
                "DataInfo/Provenance/SchemeName test not null Failed");

        assertNotNull(result.getCollectedfrom(), "CollectedFrom test not null Failed");
        assertTrue(result.getCollectedfrom().size() > 0);
        assertTrue(
                result.getCollectedfrom().stream()
                        .anyMatch(
                                c ->
                                        c.getKey()
                                                .equalsIgnoreCase(
                                                        "10|openaire____::081b82f96300b6a6e3d282bad31cb6e2")));
        assertTrue(
                result.getCollectedfrom().stream()
                        .anyMatch(c -> c.getValue().equalsIgnoreCase("crossref")));

        assertTrue(
                result.getRelevantdate().stream()
                        .anyMatch(d -> d.getQualifier().getClassid().equalsIgnoreCase("created")));
        assertTrue(
                result.getRelevantdate().stream()
                        .anyMatch(
                                d -> d.getQualifier().getClassid().equalsIgnoreCase("available")));
        assertTrue(
                result.getRelevantdate().stream()
                        .anyMatch(d -> d.getQualifier().getClassid().equalsIgnoreCase("accepted")));
        assertTrue(
                result.getRelevantdate().stream()
                        .anyMatch(
                                d ->
                                        d.getQualifier()
                                                .getClassid()
                                                .equalsIgnoreCase("published-online")));
        assertTrue(
                result.getRelevantdate().stream()
                        .anyMatch(
                                d ->
                                        d.getQualifier()
                                                .getClassid()
                                                .equalsIgnoreCase("published-print")));

        logger.info(mapper.writeValueAsString(result));
    }

    @Test
    public void testConvertArticleCrossRef2Oaf() throws IOException {

        final String json = IOUtils.toString(getClass().getResourceAsStream("article.json"));
        ObjectMapper mapper = new ObjectMapper().enable(SerializationFeature.INDENT_OUTPUT);
        assertNotNull(json);
        assertFalse(StringUtils.isBlank(json));
        final Result result = Crossref2Oaf.convert(json, logger);
        assertNotNull(result);

        assertNotNull(result.getDataInfo(), "Datainfo test not null Failed");
        assertNotNull(
                result.getDataInfo().getProvenanceaction(),
                "DataInfo/Provenance test not null Failed");
        assertFalse(
                StringUtils.isBlank(result.getDataInfo().getProvenanceaction().getClassid()),
                "DataInfo/Provenance/classId test not null Failed");
        assertFalse(
                StringUtils.isBlank(result.getDataInfo().getProvenanceaction().getClassname()),
                "DataInfo/Provenance/className test not null Failed");
        assertFalse(
                StringUtils.isBlank(result.getDataInfo().getProvenanceaction().getSchemeid()),
                "DataInfo/Provenance/SchemeId test not null Failed");
        assertFalse(
                StringUtils.isBlank(result.getDataInfo().getProvenanceaction().getSchemename()),
                "DataInfo/Provenance/SchemeName test not null Failed");

        assertNotNull(result.getCollectedfrom(), "CollectedFrom test not null Failed");
        assertTrue(result.getCollectedfrom().size() > 0);
        assertTrue(
                result.getCollectedfrom().stream()
                        .anyMatch(
                                c ->
                                        c.getKey()
                                                .equalsIgnoreCase(
                                                        "10|openaire____::081b82f96300b6a6e3d282bad31cb6e2")));
        assertTrue(
                result.getCollectedfrom().stream()
                        .anyMatch(c -> c.getValue().equalsIgnoreCase("crossref")));

        assertTrue(
                result.getRelevantdate().stream()
                        .anyMatch(d -> d.getQualifier().getClassid().equalsIgnoreCase("created")));
        assertTrue(
                result.getRelevantdate().stream()
                        .anyMatch(
                                d ->
                                        d.getQualifier()
                                                .getClassid()
                                                .equalsIgnoreCase("published-online")));
        //        assertTrue(
        //                result.getRelevantdate().stream()
        //                        .anyMatch(
        //                                d ->
        //                                        d.getQualifier()
        //                                                .getClassid()
        //                                                .equalsIgnoreCase("published-print")));

        logger.info(mapper.writeValueAsString(result));
    }

    @Test
    public void testConvertBooktCrossRef2Oaf() throws IOException {

        final String json = IOUtils.toString(getClass().getResourceAsStream("book.json"));
        ObjectMapper mapper = new ObjectMapper().enable(SerializationFeature.INDENT_OUTPUT);
        assertNotNull(json);
        assertFalse(StringUtils.isBlank(json));
        final Result result = Crossref2Oaf.convert(json, logger);
        assertNotNull(result);
        logger.info(mapper.writeValueAsString(result));

        assertNotNull(result.getDataInfo(), "Datainfo test not null Failed");
        assertNotNull(
                result.getDataInfo().getProvenanceaction(),
                "DataInfo/Provenance test not null Failed");
        assertFalse(
                StringUtils.isBlank(result.getDataInfo().getProvenanceaction().getClassid()),
                "DataInfo/Provenance/classId test not null Failed");
        assertFalse(
                StringUtils.isBlank(result.getDataInfo().getProvenanceaction().getClassname()),
                "DataInfo/Provenance/className test not null Failed");
        assertFalse(
                StringUtils.isBlank(result.getDataInfo().getProvenanceaction().getSchemeid()),
                "DataInfo/Provenance/SchemeId test not null Failed");
        assertFalse(
                StringUtils.isBlank(result.getDataInfo().getProvenanceaction().getSchemename()),
                "DataInfo/Provenance/SchemeName test not null Failed");

        assertNotNull(result.getCollectedfrom(), "CollectedFrom test not null Failed");
        assertTrue(result.getCollectedfrom().size() > 0);
        assertTrue(
                result.getCollectedfrom().stream()
                        .anyMatch(
                                c ->
                                        c.getKey()
                                                .equalsIgnoreCase(
                                                        "10|openaire____::081b82f96300b6a6e3d282bad31cb6e2")));
        assertTrue(
                result.getCollectedfrom().stream()
                        .anyMatch(c -> c.getValue().equalsIgnoreCase("crossref")));

        assertTrue(
                result.getRelevantdate().stream()
                        .anyMatch(d -> d.getQualifier().getClassid().equalsIgnoreCase("created")));

        assertTrue(
                result.getRelevantdate().stream()
                        .anyMatch(
                                d ->
                                        d.getQualifier()
                                                .getClassid()
                                                .equalsIgnoreCase("published-online")));
        assertTrue(
                result.getRelevantdate().stream()
                        .anyMatch(
                                d ->
                                        d.getQualifier()
                                                .getClassid()
                                                .equalsIgnoreCase("published-print")));
    }

    @Test
    public void testPath() throws Exception {
        final String json = IOUtils.toString(getClass().getResourceAsStream("response.json"));
        final List<String> res = JsonPath.read(json, "$.hits.hits[*]._source.blob");
        System.out.println(res.size());
    }
}
