package eu.dnetlib.dhp.application;

import org.apache.commons.io.IOUtils;
import org.junit.Test;

import java.io.ByteArrayOutputStream;
import java.util.Base64;
import java.util.zip.GZIPOutputStream;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

public class ArgumentApplicationParserTest {


    @Test
    public void testParseParameter() throws Exception {
        final String jsonConfiguration = IOUtils.toString(this.getClass().getResourceAsStream("/eu/dnetlib/application/parameters.json"));
        assertNotNull(jsonConfiguration);
        ArgumentApplicationParser parser = new ArgumentApplicationParser(jsonConfiguration);
        parser.parseArgument(new String[]{"-p", "value0",
                "-a", "value1",
                "-n", "value2",
                "-u", "value3",
                "-ru", "value4",
                "-rp", "value5",
                "-rh", "value6",
                "-ro", "value7",
                "-rr", "value8",
                "-w", "value9",
                "-cc", ArgumentApplicationParser.compressArgument(jsonConfiguration)
        });
        assertNotNull(parser.get("hdfsPath"));
        assertNotNull(parser.get("apidescriptor"));
        assertNotNull(parser.get("namenode"));
        assertNotNull(parser.get("userHDFS"));
        assertNotNull(parser.get("rabbitUser"));
        assertNotNull(parser.get("rabbitPassWord"));
        assertNotNull(parser.get("rabbitHost"));
        assertNotNull(parser.get("rabbitOngoingQueue"));
        assertNotNull(parser.get("rabbitReportQueue"));
        assertNotNull(parser.get("workflowId"));
        assertEquals("value0", parser.get("hdfsPath"));
        assertEquals("value1", parser.get("apidescriptor"));
        assertEquals("value2", parser.get("namenode"));
        assertEquals("value3", parser.get("userHDFS"));
        assertEquals("value4", parser.get("rabbitUser"));
        assertEquals("value5", parser.get("rabbitPassWord"));
        assertEquals("value6", parser.get("rabbitHost"));
        assertEquals("value7", parser.get("rabbitOngoingQueue"));
        assertEquals("value8", parser.get("rabbitReportQueue"));
        assertEquals("value9", parser.get("workflowId"));
        assertEquals(jsonConfiguration, parser.get("ccCoco"));
    }






}
