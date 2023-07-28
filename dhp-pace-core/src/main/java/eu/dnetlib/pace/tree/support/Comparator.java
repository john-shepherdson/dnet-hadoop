
package eu.dnetlib.pace.tree.support;

import eu.dnetlib.pace.config.Config;

public interface Comparator<T> {

	/*
	 * return : -1 -> can't decide (i.e. missing field) >0 -> similarity degree (depends on the algorithm)
	 */
	public double compare(Object a, Object b, Config conf);
}
