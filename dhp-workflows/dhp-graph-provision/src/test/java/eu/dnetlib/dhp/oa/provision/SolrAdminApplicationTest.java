
package eu.dnetlib.dhp.oa.provision;

import org.apache.solr.client.solrj.response.SolrPingResponse;
import org.apache.solr.client.solrj.response.UpdateResponse;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import junit.framework.Assert;

public class SolrAdminApplicationTest extends SolrTest {

	@Test
	public void testPing() throws Exception {
		SolrPingResponse pingResponse = miniCluster.getSolrClient().ping();
		log.info("pingResponse: '{}'", pingResponse.getStatus());
		Assert.assertTrue(pingResponse.getStatus() == 0);
	}

	@Test
	public void testAdminApplication_DELETE() throws Exception {

		SolrAdminApplication admin = new SolrAdminApplication(miniCluster.getSolrClient().getZkHost());

		UpdateResponse rsp = (UpdateResponse) admin
			.execute(SolrAdminApplication.Action.DELETE_BY_QUERY, DEFAULT_COLLECTION, "*:*", false);

		Assertions.assertTrue(rsp.getStatus() == 0);
	}

	@Test
	public void testAdminApplication_COMMIT() throws Exception {

		SolrAdminApplication admin = new SolrAdminApplication(miniCluster.getSolrClient().getZkHost());

		UpdateResponse rsp = (UpdateResponse) admin.commit(DEFAULT_COLLECTION);

		Assertions.assertTrue(rsp.getStatus() == 0);
	}

}
