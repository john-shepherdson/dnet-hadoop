package eu.dnetlib.dhp.broker.oa.util;

import java.io.IOException;

import co.elastic.clients.elasticsearch._types.ElasticsearchException;
import co.elastic.clients.elasticsearch.core.DeleteByQueryRequest;
import eu.dnetlib.dhp.index.es.ESFeeder;

public class BrokerIndexClient extends ESFeeder {

	public BrokerIndexClient(final String url) {
		super(url);
	}

	public void deleteUsingExactField(final String index, final String field, final String value) throws ElasticsearchException, IOException {

		final DeleteByQueryRequest req = DeleteByQueryRequest.of(b -> b
				.index(index)
				.query(q -> q.term(t -> t.field(field).value(value))));

		getEsClient().deleteByQuery(req);
	}

	public void deleteUsingDateBefore(final String index, final String field, final double date) throws ElasticsearchException, IOException {
		final DeleteByQueryRequest req = DeleteByQueryRequest.of(b -> b
				.index(index)
				.query(q -> q.range(r -> r.number(n -> n.field(field).lte(date)))));

		getEsClient().deleteByQuery(req);

	}

}
