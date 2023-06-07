package eu.dnetlib.pace.tree;

import com.wcohen.ss.AbstractStringDistance;
import eu.dnetlib.pace.tree.support.AbstractStringComparator;
import eu.dnetlib.pace.tree.support.ComparatorClass;

import java.util.Map;

@ComparatorClass("levenstein")
public class Levenstein extends AbstractStringComparator {

	public Levenstein(Map<String,String> params){
		super(params, new com.wcohen.ss.Levenstein());
	}

	public Levenstein(double w) {
		super(w, new com.wcohen.ss.Levenstein());
	}

	protected Levenstein(double w, AbstractStringDistance ssalgo) {
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
