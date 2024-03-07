
package eu.dnetlib.pace.clustering;

import java.util.Collection;
import java.util.List;
import java.util.Map;

import eu.dnetlib.pace.config.Config;

public interface ClusteringFunction {

	public Collection<String> apply(Config config, List<String> fields);

	public Map<String, Object> getParams();

}
