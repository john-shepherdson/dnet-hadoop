
package eu.dnetlib.dhp.oa.provision;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.apache.solr.client.solrj.response.SolrPingResponse;
import org.apache.solr.client.solrj.response.UpdateResponse;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

class SolrAdminApplicationTest extends SolrTest {

	@Test
	void testPing() throws Exception {
		SolrPingResponse pingResponse = miniCluster.getSolrClient().ping();
		log.info("pingResponse: '{}'", pingResponse.getStatus());
		assertEquals(0, pingResponse.getStatus());
	}

	@Test
	void testAdminApplication_DELETE() throws Exception {

		SolrAdminApplication admin = new SolrAdminApplication(miniCluster.getSolrClient().getZkHost());

		UpdateResponse rsp = (UpdateResponse) admin
			.execute(SolrAdminApplication.Action.DELETE_BY_QUERY, DEFAULT_COLLECTION, "*:*", false);

		assertEquals(0, rsp.getStatus());
	}

	@Test
	void testAdminApplication_COMMIT() throws Exception {

		SolrAdminApplication admin = new SolrAdminApplication(miniCluster.getSolrClient().getZkHost());

		UpdateResponse rsp = (UpdateResponse) admin.commit(DEFAULT_COLLECTION);

		assertEquals(0, rsp.getStatus());
	}

}
