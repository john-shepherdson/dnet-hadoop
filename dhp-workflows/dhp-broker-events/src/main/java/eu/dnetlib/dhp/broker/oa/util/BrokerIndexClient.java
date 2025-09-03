package eu.dnetlib.dhp.broker.oa.util;

import java.io.IOException;

import co.elastic.clients.elasticsearch._types.Conflicts;
import co.elastic.clients.elasticsearch._types.ElasticsearchException;
import co.elastic.clients.elasticsearch.core.DeleteByQueryRequest;
import co.elastic.clients.elasticsearch.core.DeleteByQueryResponse;
import co.elastic.clients.elasticsearch.tasks.GetTasksResponse;
import eu.dnetlib.dhp.index.es.ESFeeder;

public class BrokerIndexClient extends ESFeeder {

	public BrokerIndexClient(final String url) {
		super(url);
	}

	public void deleteUsingExactField(final String index, final String field, final String value, final boolean waitTermination)
			throws ElasticsearchException, IOException {

		final DeleteByQueryRequest req = DeleteByQueryRequest.of(b -> b
				.index(index)
				.waitForCompletion(false)
				.conflicts(Conflicts.Proceed)
				.query(q -> q.term(t -> t.field(field).value(value))));

		final DeleteByQueryResponse res = getEsClient().deleteByQuery(req);

		if (waitTermination) {
			waitForTaskCompletion(res.task());
		}

	}

	public void deleteUsingDateBefore(final String index, final String field, final double date, final boolean waitTermination)
			throws ElasticsearchException, IOException {
		final DeleteByQueryRequest req = DeleteByQueryRequest.of(b -> b
				.index(index)
				.waitForCompletion(false)
				.conflicts(Conflicts.Proceed)
				.query(q -> q.range(r -> r.number(n -> n.field(field).lte(date)))));

		getEsClient().deleteByQuery(req);

		final DeleteByQueryResponse res = getEsClient().deleteByQuery(req);

		if (waitTermination) {
			waitForTaskCompletion(res.task());
		}
	}

	private void waitForTaskCompletion(final String task) throws ElasticsearchException, IOException {

		while (true) {
			final GetTasksResponse taskStatus = getEsClient().tasks().get(t -> t.taskId(task));
			if ((taskStatus == null) || taskStatus.completed()) {
				break;
			}
			try {
				Thread.sleep(60000);
			} catch (final InterruptedException e) {
				Thread.currentThread().interrupt();
				throw new RuntimeException(e);
			}
		}
	}

}
