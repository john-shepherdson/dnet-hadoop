package eu.dnetlib.pace.config;

import java.util.List;
import java.util.Map;
import java.util.function.Predicate;
import java.util.regex.Pattern;

import eu.dnetlib.pace.model.ClusteringDef;
import eu.dnetlib.pace.model.FieldDef;
import eu.dnetlib.pace.tree.support.TreeNodeDef;

/**
 * Interface for PACE configuration bean.
 *
 * @author claudio
 */
public interface Config {

	/**
	 * Field configuration definitions.
	 *
	 * @return the list of definitions
	 */
	public List<FieldDef> model();

	/**
	 * Decision Tree definition
	 *
	 * @return the map representing the decision tree
	 */
	public Map<String, TreeNodeDef> decisionTree();

	/**
	 * Clusterings.
	 *
	 * @return the list
	 */
	public List<ClusteringDef> clusterings();

	/**
	 * Blacklists.
	 *
	 * @return the map
	 */
	public Map<String, Predicate<String>> blacklists();


	/**
	 * Translation map.
	 *
	 * @return the map
	 * */
	public Map<String, String> translationMap();
}
