
package eu.dnetlib.pace.tree.support;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.spark.sql.Row;

import eu.dnetlib.pace.config.Config;
import eu.dnetlib.pace.util.PaceException;

/**
 * The compare between two documents is given by the weighted mean of the field distances
 */
public class TreeProcessor {

	private static final Log log = LogFactory.getLog(TreeProcessor.class);

	private Config config;

	public TreeProcessor(final Config config) {
		this.config = config;
	}

	// row based copies

	public boolean compare(final Row a, final Row b) {
		// evaluate the decision tree
		return evaluateTree(a, b).getResult() == MatchType.MATCH;
	}

	public TreeStats evaluateTree(final Row doc1, final Row doc2) {

		TreeStats treeStats = new TreeStats();

		String nextNodeName = "start";

		do {

			TreeNodeDef currentNode = config.decisionTree().get(nextNodeName);
			// throw an exception if the node doesn't exist
			if (currentNode == null)
				throw new PaceException("Missing tree node: " + nextNodeName);

			TreeNodeStats stats = currentNode.evaluate(doc1, doc2, config);
			treeStats.addNodeStats(nextNodeName, stats);

			double finalScore = stats.getFinalScore(currentNode.getAggregation());
			if (finalScore == -1.0)
				nextNodeName = currentNode.getUndefined();
			else if (finalScore >= currentNode.getThreshold()) {
				nextNodeName = currentNode.getPositive();
			} else {
				nextNodeName = currentNode.getNegative();
			}

		} while (MatchType.parse(nextNodeName) == MatchType.UNDEFINED);

		treeStats.setResult(MatchType.parse(nextNodeName));
		return treeStats;
	}

	public double computeScore(final Row doc1, final Row doc2) {
		String nextNodeName = "start";
		double score = 0.0;

		do {

			TreeNodeDef currentNode = config.decisionTree().get(nextNodeName);
			// throw an exception if the node doesn't exist
			if (currentNode == null)
				throw new PaceException("The Tree Node doesn't exist: " + nextNodeName);

			TreeNodeStats stats = currentNode.evaluate(doc1, doc2, config);

			score = stats.getFinalScore(currentNode.getAggregation());
			// if ignoreUndefined=false the miss is considered as undefined
			if (!currentNode.isIgnoreUndefined() && stats.undefinedCount() > 0) {
				nextNodeName = currentNode.getUndefined();
			}
			// if ignoreUndefined=true the miss is ignored and the score computed anyway
			else if (stats.getFinalScore(currentNode.getAggregation()) >= currentNode.getThreshold()) {
				nextNodeName = currentNode.getPositive();
			} else {
				nextNodeName = currentNode.getNegative();
			}
		} while (MatchType.parse(nextNodeName) == MatchType.UNDEFINED);

		return score;
	}
}
