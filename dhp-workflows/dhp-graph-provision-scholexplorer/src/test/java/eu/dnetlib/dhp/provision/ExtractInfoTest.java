package eu.dnetlib.dhp.provision;

import com.fasterxml.jackson.databind.ObjectMapper;
import eu.dnetlib.dhp.provision.scholix.Scholix;
import eu.dnetlib.dhp.provision.scholix.summary.ScholixSummary;
import org.apache.commons.io.IOUtils;
import org.junit.jupiter.api.Test;

public class ExtractInfoTest {
  @Test
  public void testSerialization() throws Exception {

    ScholixSummary summary = new ScholixSummary();
    summary.setDescription("descrizione");
    ObjectMapper mapper = new ObjectMapper();
    String json = mapper.writeValueAsString(summary);
    System.out.println(json);
    System.out.println(mapper.readValue(json, ScholixSummary.class).getDescription());
  }

  @Test
  public void testScholix() throws Exception {
    final String jsonSummary = IOUtils.toString(getClass().getResourceAsStream("summary.json"));
    final String jsonRelation = IOUtils.toString(getClass().getResourceAsStream("relation.json"));
    Scholix.generateScholixWithSource(jsonSummary, jsonRelation);
  }
}
