package eu.dnetlib.dhp.schema.proto;

import com.googlecode.protobuf.format.JsonFormat;
import eu.dnetlib.data.proto.OafProtos;
import org.apache.commons.io.IOUtils;
import org.junit.Test;


public class TestParseProtoJson {


    @Test
    public void testParse() throws Exception {
        final String json = IOUtils.toString(this.getClass().getResourceAsStream("/eu/dnetlib/dhp/schema/proto/hugeRecord.json"));

        OafProtos.Oaf.Builder oafBuilder =OafProtos.Oaf.newBuilder();
        JsonFormat.merge(json,oafBuilder);

        System.out.println(JsonFormat.printToString(oafBuilder.build()));

    }
}
