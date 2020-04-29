package eu.dnetlib.dhp.sx.graph;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import eu.dnetlib.dhp.schema.oaf.Oaf;
import eu.dnetlib.dhp.sx.graph.parser.DatasetScholexplorerParser;
import eu.dnetlib.scholexplorer.relation.RelationMapper;
import java.util.List;
import org.apache.commons.io.IOUtils;
import org.junit.jupiter.api.Test;

public class ScholexplorerParserTest {

  @Test
  public void testDataciteParser() throws Exception {
    String xml = IOUtils.toString(this.getClass().getResourceAsStream("dmf.xml"));

    DatasetScholexplorerParser p = new DatasetScholexplorerParser();
    List<Oaf> oaves = p.parseObject(xml, RelationMapper.load());

    ObjectMapper m = new ObjectMapper();
    m.enable(SerializationFeature.INDENT_OUTPUT);

    oaves.forEach(
        oaf -> {
          try {
            System.out.println(m.writeValueAsString(oaf));
            System.out.println("----------------------------");
          } catch (JsonProcessingException e) {

          }
        });
  }
}
