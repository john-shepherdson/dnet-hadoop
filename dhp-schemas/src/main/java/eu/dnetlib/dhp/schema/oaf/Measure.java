
package eu.dnetlib.dhp.schema.oaf;

import java.io.Serializable;
import java.util.List;

import com.google.common.base.Objects;

/**
 * Represent a measure, must be further described by a system available resource providing name and descriptions.
 */
public class Measure implements Serializable {

	/**
	 * Unique measure identifier.
	 */
	private String id;

	/**
	 * List of units associated with this measure. KeyValue provides a pair to store the label (key) and the value, plus
	 * common provenance information.
	 */
	private List<KeyValue> unit;

	public String getId() {
		return id;
	}

	public void setId(String id) {
		this.id = id;
	}

	public List<KeyValue> getUnit() {
		return unit;
	}

	public void setUnit(List<KeyValue> unit) {
		this.unit = unit;
	}

	public void mergeFrom(Measure m) {
		// TODO
	}

	@Override
	public boolean equals(Object o) {
		if (this == o)
			return true;
		if (o == null || getClass() != o.getClass())
			return false;
		Measure measure = (Measure) o;
		return Objects.equal(id, measure.id) &&
			Objects.equal(unit, measure.unit);
	}

	@Override
	public int hashCode() {
		return Objects.hashCode(id, unit);
	}
}
