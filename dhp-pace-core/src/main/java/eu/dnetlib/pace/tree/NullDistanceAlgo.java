
package eu.dnetlib.pace.tree;

import java.util.Map;

import eu.dnetlib.pace.config.Config;
import eu.dnetlib.pace.tree.support.Comparator;
import eu.dnetlib.pace.tree.support.ComparatorClass;

/**
 * Not all fields of a document need to partecipate in the compare measure. We model those fields as having a
 * NullDistanceAlgo.
 */
@ComparatorClass("null")
public class NullDistanceAlgo<T> implements Comparator<T> {

	public NullDistanceAlgo(Map<String, String> params) {
	}

	@Override
	public double compare(Object a, Object b, Config config) {
		return 0;
	}
}
