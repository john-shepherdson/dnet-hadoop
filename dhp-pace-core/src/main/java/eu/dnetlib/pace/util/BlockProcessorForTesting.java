
package eu.dnetlib.pace.util;

import java.util.*;

import org.apache.commons.lang3.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.google.common.collect.Lists;

import eu.dnetlib.pace.clustering.NGramUtils;
import eu.dnetlib.pace.config.DedupConfig;
import eu.dnetlib.pace.config.WfConfig;
import eu.dnetlib.pace.model.Field;
import eu.dnetlib.pace.model.MapDocument;
import eu.dnetlib.pace.model.MapDocumentComparator;
import eu.dnetlib.pace.tree.*;
import eu.dnetlib.pace.tree.support.TreeProcessor;

public class BlockProcessorForTesting {

	public static final List<String> accumulators = new ArrayList<>();

	private static final Log log = LogFactory.getLog(eu.dnetlib.pace.util.BlockProcessorForTesting.class);

	private DedupConfig dedupConf;

	public static void constructAccumulator(final DedupConfig dedupConf) {
		accumulators.add(String.format("%s::%s", dedupConf.getWf().getEntityType(), "records per hash key = 1"));
		accumulators
			.add(
				String
					.format(
						"%s::%s", dedupConf.getWf().getEntityType(), "missing " + dedupConf.getWf().getOrderField()));
		accumulators
			.add(
				String
					.format(
						"%s::%s", dedupConf.getWf().getEntityType(),
						String
							.format(
								"Skipped records for count(%s) >= %s", dedupConf.getWf().getOrderField(),
								dedupConf.getWf().getGroupMaxSize())));
		accumulators.add(String.format("%s::%s", dedupConf.getWf().getEntityType(), "skip list"));
		accumulators.add(String.format("%s::%s", dedupConf.getWf().getEntityType(), "dedupSimilarity (x2)"));
		accumulators
			.add(String.format("%s::%s", dedupConf.getWf().getEntityType(), "d < " + dedupConf.getWf().getThreshold()));
	}

	public BlockProcessorForTesting(DedupConfig dedupConf) {
		this.dedupConf = dedupConf;
	}

	public void processSortedBlock(final String key, final List<MapDocument> documents, final Reporter context,
		boolean useTree, boolean noMatch) {
		if (documents.size() > 1) {
//            log.info("reducing key: '" + key + "' records: " + q.size());
			process(prepare(documents), context, useTree, noMatch);

		} else {
			context.incrementCounter(dedupConf.getWf().getEntityType(), "records per hash key = 1", 1);
		}
	}

	public void process(final String key, final Iterable<MapDocument> documents, final Reporter context,
		boolean useTree, boolean noMatch) {

		final Queue<MapDocument> q = prepare(documents);

		if (q.size() > 1) {
//            log.info("reducing key: '" + key + "' records: " + q.size());
			process(simplifyQueue(q, key, context), context, useTree, noMatch);

		} else {
			context.incrementCounter(dedupConf.getWf().getEntityType(), "records per hash key = 1", 1);
		}
	}

	private Queue<MapDocument> prepare(final Iterable<MapDocument> documents) {
		final Queue<MapDocument> queue = new PriorityQueue<>(100,
			new MapDocumentComparator(dedupConf.getWf().getOrderField()));

		final Set<String> seen = new HashSet<String>();
		final int queueMaxSize = dedupConf.getWf().getQueueMaxSize();

		documents.forEach(doc -> {
			if (queue.size() <= queueMaxSize) {
				final String id = doc.getIdentifier();

				if (!seen.contains(id)) {
					seen.add(id);
					queue.add(doc);
				}
			}
		});

		return queue;
	}

	private Queue<MapDocument> simplifyQueue(final Queue<MapDocument> queue, final String ngram,
		final Reporter context) {
		final Queue<MapDocument> q = new LinkedList<>();

		String fieldRef = "";
		final List<MapDocument> tempResults = Lists.newArrayList();

		while (!queue.isEmpty()) {
			final MapDocument result = queue.remove();

			final String orderFieldName = dedupConf.getWf().getOrderField();
			final Field orderFieldValue = result.values(orderFieldName);
			if (!orderFieldValue.isEmpty()) {
				final String field = NGramUtils.cleanupForOrdering(orderFieldValue.stringValue());
				if (field.equals(fieldRef)) {
					tempResults.add(result);
				} else {
					populateSimplifiedQueue(q, tempResults, context, fieldRef, ngram);
					tempResults.clear();
					tempResults.add(result);
					fieldRef = field;
				}
			} else {
				context
					.incrementCounter(
						dedupConf.getWf().getEntityType(), "missing " + dedupConf.getWf().getOrderField(), 1);
			}
		}
		populateSimplifiedQueue(q, tempResults, context, fieldRef, ngram);

		return q;
	}

	private void populateSimplifiedQueue(final Queue<MapDocument> q,
		final List<MapDocument> tempResults,
		final Reporter context,
		final String fieldRef,
		final String ngram) {
		WfConfig wf = dedupConf.getWf();
		if (tempResults.size() < wf.getGroupMaxSize()) {
			q.addAll(tempResults);
		} else {
			context
				.incrementCounter(
					wf.getEntityType(),
					String.format("Skipped records for count(%s) >= %s", wf.getOrderField(), wf.getGroupMaxSize()),
					tempResults.size());
//            log.info("Skipped field: " + fieldRef + " - size: " + tempResults.size() + " - ngram: " + ngram);
		}
	}

	private void process(final Queue<MapDocument> queue, final Reporter context, boolean useTree, boolean noMatch) {

		while (!queue.isEmpty()) {

			final MapDocument pivot = queue.remove();
			final String idPivot = pivot.getIdentifier();

			WfConfig wf = dedupConf.getWf();
			final Field fieldsPivot = pivot.values(wf.getOrderField());
			final String fieldPivot = (fieldsPivot == null) || fieldsPivot.isEmpty() ? "" : fieldsPivot.stringValue();

			if (fieldPivot != null) {
				int i = 0;
				for (final MapDocument curr : queue) {
					final String idCurr = curr.getIdentifier();

					if (mustSkip(idCurr)) {

						context.incrementCounter(wf.getEntityType(), "skip list", 1);

						break;
					}

					if (i > wf.getSlidingWindowSize()) {
						break;
					}

					final Field fieldsCurr = curr.values(wf.getOrderField());
					final String fieldCurr = (fieldsCurr == null) || fieldsCurr.isEmpty() ? null
						: fieldsCurr.stringValue();

					if (!idCurr.equals(idPivot) && (fieldCurr != null)) {

						// draws no match relations (test purpose)
						if (noMatch) {
							emitOutput(!new TreeProcessor(dedupConf).compare(pivot, curr), idPivot, idCurr, context);
						} else {
							// use the decision tree implementation or the "normal" implementation of the similarity
							// score (valid only for publications)
							if (useTree)
								emitOutput(new TreeProcessor(dedupConf).compare(pivot, curr), idPivot, idCurr, context);
							else
								emitOutput(publicationCompare(pivot, curr, dedupConf), idPivot, idCurr, context);
						}
//                            if(new TreeProcessor(dedupConf).compare(pivot, curr) != publicationCompare(pivot, curr, dedupConf)) {
//                                emitOutput(true, idPivot, idCurr, context);
//                            }

					}
				}
			}
		}
	}

	protected static boolean compareInstanceType(MapDocument a, MapDocument b, DedupConfig conf) {
		Map<String, String> params = new HashMap<>();
		InstanceTypeMatch instanceTypeMatch = new InstanceTypeMatch(params);
		double compare = instanceTypeMatch
			.compare(a.getFieldMap().get("instance"), b.getFieldMap().get("instance"), conf);
		return compare >= 1.0;
	}

	private boolean publicationCompare(MapDocument a, MapDocument b, DedupConfig config) {
		// if the score gives 1, the publications are equivalent
		Map<String, String> params = new HashMap<>();
		params.put("jpath_value", "$.value");
		params.put("jpath_classid", "$.qualifier.classid");
		params.put("mode", "count");

		double score = 0.0;

		// levenstein title
		LevensteinTitle levensteinTitle = new LevensteinTitle(params);
		if (levensteinTitle.compare(a.getFieldMap().get("title"), b.getFieldMap().get("title"), config) >= 0.9) {
			score += 0.2;
		}

		// pid
		JsonListMatch jsonListMatch = new JsonListMatch(params);
		if (jsonListMatch.compare(a.getFieldMap().get("pid"), b.getFieldMap().get("pid"), config) >= 1.0) {
			score += 0.5;
		}

		// title version
		TitleVersionMatch titleVersionMatch = new TitleVersionMatch(params);
		double result1 = titleVersionMatch.compare(a.getFieldMap().get("title"), b.getFieldMap().get("title"), config);
		if (result1 < 0 || result1 >= 1.0) {
			score += 0.1;
		}

		// authors match
		params.remove("mode");
		AuthorsMatch authorsMatch = new AuthorsMatch(params);
		double result2 = authorsMatch.compare(a.getFieldMap().get("authors"), b.getFieldMap().get("authors"), config);
		if (result2 < 0 || result2 >= 0.6) {
			score += 0.2;
		}

		return score >= 0.5;
	}

	private void emitOutput(final boolean result, final String idPivot, final String idCurr, final Reporter context) {

		if (result) {
			writeSimilarity(context, idPivot, idCurr);
			context.incrementCounter(dedupConf.getWf().getEntityType(), "dedupSimilarity (x2)", 1);
		} else {
			context.incrementCounter(dedupConf.getWf().getEntityType(), "d < " + dedupConf.getWf().getThreshold(), 1);
		}
	}

	private boolean mustSkip(final String idPivot) {
		return dedupConf.getWf().getSkipList().contains(getNsPrefix(idPivot));
	}

	private String getNsPrefix(final String id) {
		return StringUtils.substringBetween(id, "|", "::");
	}

	private void writeSimilarity(final Reporter context, final String from, final String to) {
		final String type = dedupConf.getWf().getEntityType();

		context.emit(type, from, to);
	}
}
