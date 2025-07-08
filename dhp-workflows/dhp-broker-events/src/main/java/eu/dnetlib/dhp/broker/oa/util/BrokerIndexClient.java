package eu.dnetlib.dhp.broker.oa.util;

import java.io.IOException;

import co.elastic.clients.elasticsearch._types.ElasticsearchException;
import co.elastic.clients.elasticsearch.core.DeleteByQueryRequest;
import eu.dnetlib.dhp.index.es.ESFeeder;

public class BrokerIndexClient extends ESFeeder {

	public BrokerIndexClient(final String url) {
		super(url);
	}

	public void deleteAlertNotifications(final String index, final String dsId) throws ElasticsearchException, IOException {

		final DeleteByQueryRequest req = DeleteByQueryRequest.of(b -> b
				.index(index)
				.query(q -> q.term(t -> t.field("map.datasourceId").value(dsId))));

		getEsClient().deleteByQuery(req);
	}

}
