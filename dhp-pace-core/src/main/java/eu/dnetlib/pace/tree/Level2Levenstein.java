
package eu.dnetlib.pace.tree;

import java.util.Map;

import com.wcohen.ss.AbstractStringDistance;

import eu.dnetlib.pace.tree.support.AbstractStringComparator;
import eu.dnetlib.pace.tree.support.ComparatorClass;

@ComparatorClass("level2Levenstein")
public class Level2Levenstein extends AbstractStringComparator {

	public Level2Levenstein(Map<String, String> params) {
		super(params, new com.wcohen.ss.Level2Levenstein());
	}

	public Level2Levenstein(double w) {
		super(w, new com.wcohen.ss.Level2Levenstein());
	}

	protected Level2Levenstein(double w, AbstractStringDistance ssalgo) {
		super(w, ssalgo);
	}

	@Override
	public double getWeight() {
		return super.weight;
	}

	@Override
	protected double normalize(double d) {
		return 1 / Math.pow(Math.abs(d) + 1, 0.1);
	}

}
