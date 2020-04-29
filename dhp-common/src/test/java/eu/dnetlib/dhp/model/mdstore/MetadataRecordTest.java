package eu.dnetlib.dhp.model.mdstore;

import static org.junit.jupiter.api.Assertions.assertTrue;

import org.junit.jupiter.api.Test;

public class MetadataRecordTest {

  @Test
  public void getTimestamp() {

    MetadataRecord r = new MetadataRecord();
    assertTrue(r.getDateOfCollection() > 0);
  }
}
