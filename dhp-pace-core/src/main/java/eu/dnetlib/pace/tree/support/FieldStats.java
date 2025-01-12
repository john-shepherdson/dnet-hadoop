
package eu.dnetlib.pace.tree.support;

import java.io.IOException;
import java.io.Serializable;

import com.fasterxml.jackson.databind.ObjectMapper;

import eu.dnetlib.pace.util.PaceException;

/**
 * The class that contains the result of each comparison in the decision tree
 * */
public class FieldStats implements Serializable {

	private double weight; // weight for the field (to be used in the aggregation)
	private double threshold; // threshold for the field (to be used in some kind of aggregations)
	private double result; // the result of the comparison
	private Object a;
	private Object b;

	private boolean countIfUndefined;

	public FieldStats(double weight, double threshold, double result, boolean countIfUndefined, Object a, Object b) {
		this.weight = weight;
		this.threshold = threshold;
		this.result = result;
		this.countIfUndefined = countIfUndefined;
		this.a = a;
		this.b = b;
	}

	public double getThreshold() {
		return threshold;
	}

	public void setThreshold(double threshold) {
		this.threshold = threshold;
	}

	public double getWeight() {
		return weight;
	}

	public void setWeight(double weight) {
		this.weight = weight;
	}

	public double getResult() {
		return result;
	}

	public void setResult(double result) {
		this.result = result;
	}

	public boolean isCountIfUndefined() {
		return countIfUndefined;
	}

	public void setCountIfUndefined(boolean countIfUndefined) {
		this.countIfUndefined = countIfUndefined;
	}

	public Object getA() {
		return a;
	}

	public void setA(Object a) {
		this.a = a;
	}

	public Object getB() {
		return b;
	}

	public void setB(Object b) {
		this.b = b;
	}

	@Override
	public String toString() {
		try {
			return new ObjectMapper().writeValueAsString(this);
		} catch (IOException e) {
			throw new PaceException("Impossible to convert to JSON: ", e);
		}
	}
}
