
package eu.dnetlib.pace.tree;

import java.util.Map;

import com.wcohen.ss.AbstractStringDistance;

import eu.dnetlib.pace.config.Config;
import eu.dnetlib.pace.tree.support.AbstractStringComparator;
import eu.dnetlib.pace.tree.support.ComparatorClass;

//case class JaroWinkler(w: Double) extends SecondStringDistanceAlgo(w, new com.wcohen.ss.JaroWinkler())
@ComparatorClass("jaroWinkler")
public class JaroWinkler extends AbstractStringComparator {

	public JaroWinkler(Map<String, String> params) {
		super(params, new com.wcohen.ss.JaroWinkler());
	}

	public JaroWinkler(double weight) {
		super(weight, new com.wcohen.ss.JaroWinkler());
	}

	protected JaroWinkler(double weight, AbstractStringDistance ssalgo) {
		super(weight, ssalgo);
	}

	@Override
	public double distance(String a, String b, final Config conf) {
		String ca = cleanup(a);
		String cb = cleanup(b);

		return normalize(ssalgo.score(ca, cb));
	}

	@Override
	public double getWeight() {
		return super.weight;
	}

	@Override
	protected double normalize(double d) {
		return d;
	}

}
