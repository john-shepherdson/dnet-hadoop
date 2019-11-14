package eu.dnetlib.pace.tree.support;

import eu.dnetlib.pace.config.Config;
import eu.dnetlib.pace.model.*;
import eu.dnetlib.pace.util.PaceException;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.util.Map;

/**
 * The compare between two documents is given by the weighted mean of the field distances
 */
public class TreeProcessor {

	private static final Log log = LogFactory.getLog(TreeProcessor.class);

	private Config config;

	public TreeProcessor(final Config config) {
		this.config = config;
	}

	public boolean compare(final MapDocument a, final MapDocument b) {

		//evaluate the decision tree
		return evaluateTree(a, b) == MatchType.MATCH;
	}

	public MatchType evaluateTree(final MapDocument doc1, final MapDocument doc2){

		String current = "start";

		while (MatchType.parse(current)==MatchType.UNDEFINED) {

			TreeNodeDef currentNode = config.decisionTree().get(current);
			//throw an exception if the node doesn't exist
			if (currentNode == null)
				throw new PaceException("The Tree Node doesn't exist: " + current);

			TreeNodeStats stats = currentNode.evaluate(doc1, doc2, config);

			//if ignoreUndefined=false the miss is considered as undefined
			if (!currentNode.isIgnoreUndefined() && stats.getUndefinedCount()>0) {
				current = currentNode.getUndefined();
			}
			//if ignoreUndefined=true the miss is ignored and the score computed anyway
			else if (stats.getFinalScore(currentNode.getAggregation()) >= currentNode.getThreshold()) {
				current = currentNode.getPositive();
			}
			else {
				current = currentNode.getNegative();
			}

		}

		return MatchType.parse(current);
	}

	public double computeScore(final MapDocument doc1, final MapDocument doc2) {
		String current = "start";
		double score = 0.0;

		while (MatchType.parse(current)==MatchType.UNDEFINED) {

			TreeNodeDef currentNode = config.decisionTree().get(current);
			//throw an exception if the node doesn't exist
			if (currentNode == null)
				throw new PaceException("The Tree Node doesn't exist: " + current);

			TreeNodeStats stats = currentNode.evaluate(doc1, doc2, config);

			score = stats.getFinalScore(currentNode.getAggregation());
			//if ignoreUndefined=false the miss is considered as undefined
			if (!currentNode.isIgnoreUndefined() && stats.getUndefinedCount()>0) {
				current = currentNode.getUndefined();
			}
			//if ignoreUndefined=true the miss is ignored and the score computed anyway
			else if (stats.getFinalScore(currentNode.getAggregation()) >= currentNode.getThreshold()) {
				current = currentNode.getPositive();
			}
			else {
				current = currentNode.getNegative();
			}

		}

		return score;
	}

}
