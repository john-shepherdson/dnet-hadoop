
package eu.dnetlib.pace.tree.support;

import java.io.IOException;
import java.io.Serializable;
import java.util.List;

import org.apache.spark.sql.Row;
import org.apache.spark.sql.types.ArrayType;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.StringType;

import com.fasterxml.jackson.databind.ObjectMapper;

import eu.dnetlib.pace.config.Config;
import eu.dnetlib.pace.config.PaceConfig;
import eu.dnetlib.pace.util.PaceException;

public class TreeNodeDef implements Serializable {

	final static String CROSS_COMPARE = "crossCompare";

	private List<FieldConf> fields;
	private AggType aggregation;

	private double threshold;

	private String positive;
	private String negative;
	private String undefined;

	boolean ignoreUndefined;

	public TreeNodeDef(List<FieldConf> fields, AggType aggregation, double threshold, String positive, String negative,
		String undefined, boolean ignoreUndefined) {
		this.fields = fields;
		this.aggregation = aggregation;
		this.threshold = threshold;
		this.positive = positive;
		this.negative = negative;
		this.undefined = undefined;
		this.ignoreUndefined = ignoreUndefined;
	}

	public TreeNodeDef() {
	}

	// function for the evaluation of the node
	public TreeNodeStats evaluate(Row doc1, Row doc2, Config conf) {

		TreeNodeStats stats = new TreeNodeStats(ignoreUndefined);

		// for each field in the node, it computes the
		for (FieldConf fieldConf : fields) {
			double weight = fieldConf.getWeight();
			double result;

			Object value1 = getJavaValue(doc1, fieldConf.getField());
			Object value2 = getJavaValue(doc2, fieldConf.getField());

			// if the param specifies a cross comparison (i.e. compare elements from different fields), compute the
			// result for both sides and return the maximum
			String crossField = fieldConf.getParams().get(CROSS_COMPARE);
			if (crossField != null) {
				double result1 = comparator(fieldConf).compare(value1, getJavaValue(doc2, crossField), conf);
				double result2 = comparator(fieldConf).compare(getJavaValue(doc1, crossField), value2, conf);
				result = Math.max(result1, result2);
			} else {
				result = comparator(fieldConf).compare(value1, value2, conf);
			}

			stats
				.addFieldStats(
					fieldConf.getComparator() + " on " + fieldConf.getField() + " " + fields.indexOf(fieldConf),
					new FieldStats(
						weight,
						Double.parseDouble(fieldConf.getParams().getOrDefault("threshold", "1.0")),
						result,
						fieldConf.isCountIfUndefined(),
						value1,
						value2));
		}

		return stats;
	}

	public Object getJavaValue(Row row, String name) {
		int pos = row.fieldIndex(name);
		if (pos >= 0) {
			DataType dt = row.schema().fields()[pos].dataType();
			if (dt instanceof StringType) {
				return row.getString(pos);
			} else if (dt instanceof ArrayType) {
				return row.getList(pos);
			}
		}

		return null;
	}

	private Comparator comparator(final FieldConf field) {

		return PaceConfig.resolver.getComparator(field.getComparator(), field.getParams());
	}

	public List<FieldConf> getFields() {
		return fields;
	}

	public void setFields(List<FieldConf> fields) {
		this.fields = fields;
	}

	public AggType getAggregation() {
		return aggregation;
	}

	public void setAggregation(AggType aggregation) {
		this.aggregation = aggregation;
	}

	public double getThreshold() {
		return threshold;
	}

	public void setThreshold(double threshold) {
		this.threshold = threshold;
	}

	public String getPositive() {
		return positive;
	}

	public void setPositive(String positive) {
		this.positive = positive;
	}

	public String getNegative() {
		return negative;
	}

	public void setNegative(String negative) {
		this.negative = negative;
	}

	public String getUndefined() {
		return undefined;
	}

	public void setUndefined(String undefined) {
		this.undefined = undefined;
	}

	public boolean isIgnoreUndefined() {
		return ignoreUndefined;
	}

	public void setIgnoreUndefined(boolean ignoreUndefined) {
		this.ignoreUndefined = ignoreUndefined;
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
