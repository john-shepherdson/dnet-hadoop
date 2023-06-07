package eu.dnetlib.pace.clustering;

import java.util.*;

import com.google.common.base.Joiner;
import com.google.common.base.Splitter;
import com.google.common.collect.Lists;
import eu.dnetlib.pace.config.Config;

@ClusteringClass("sortedngrampairs")
public class SortedNgramPairs extends NgramPairs {

	public SortedNgramPairs(Map<String, Integer> params) {
		super(params, true);
	}

}
