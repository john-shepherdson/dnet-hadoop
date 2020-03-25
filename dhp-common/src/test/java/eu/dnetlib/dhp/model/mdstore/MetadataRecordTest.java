package eu.dnetlib.dhp.model.mdstore;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertTrue;

public class MetadataRecordTest {

    @Test
    public void getTimestamp() {

        MetadataRecord r = new MetadataRecord();
        assertTrue(r.getDateOfCollection() >0);
    }
}