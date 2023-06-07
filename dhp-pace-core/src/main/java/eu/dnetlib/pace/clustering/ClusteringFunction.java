package eu.dnetlib.pace.clustering;

import eu.dnetlib.pace.config.Config;

import java.util.Collection;
import java.util.List;
import java.util.Map;

public interface ClusteringFunction {
	
	public Collection<String> apply(Config config, List<String> fields);
	
	public Map<String, Integer> getParams();

}
