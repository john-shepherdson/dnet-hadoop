package eu.dnetlib.dhp.oa.provision.model;

import org.apache.solr.common.SolrInputDocument;
import org.apache.solr.common.SolrInputField;

import java.util.HashMap;
import java.util.Map;

public class SerializableSolrInputDocument extends SolrInputDocument {

    public SerializableSolrInputDocument() {
        super(new HashMap<>());
    }

    public SerializableSolrInputDocument(Map<String, SolrInputField> fields) {
        super(fields);
    }

}
