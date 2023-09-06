
package eu.dnetlib.pace.tree.support;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import com.fasterxml.jackson.databind.ObjectMapper;

import eu.dnetlib.pace.util.PaceException;

public class TreeStats {

	// <layer_id, <field:comparator, result>>
	Map<String, TreeNodeStats> stats;
	MatchType result;

	public TreeStats() {
		this.stats = new HashMap<>();
		this.result = MatchType.NO_MATCH;
	}

	public MatchType getResult() {
		return this.result;
	}

	public void setResult(MatchType result) {
		this.result = result;
	}

	public Map<String, TreeNodeStats> getStats() {
		return stats;
	}

	public void setStats(Map<String, TreeNodeStats> stats) {
		this.stats = stats;
	}

	public void addNodeStats(String layerID, TreeNodeStats treeNodeStats) {
		this.stats.put(layerID, treeNodeStats);
	}

	@Override
	public String toString() {
		try {
			return new ObjectMapper().writerWithDefaultPrettyPrinter().writeValueAsString(this);
		} catch (IOException e) {
			throw new PaceException("Impossible to convert to JSON: ", e);
		}
	}

}
