
package eu.dnetlib.dhp.schema.dump.oaf;

import com.fasterxml.jackson.annotation.JsonIgnore;
import org.apache.commons.lang3.StringUtils;

import java.io.Serializable;

public class Qualifier implements Serializable {

	private String code; //the classid in the Qualifier
	private String label; //the classname in the Qualifier

	public String getCode() {
		return code;
	}

	public void setCode(String code) {
		this.code = code;
	}

	public String getLabel() {
		return label;
	}

	public void setLabel(String label) {
		this.label = label;
	}

	public String toComparableString() {
		return isBlank()
			? ""
			: String
				.format(
					"%s::%s::%s::%s",
					code != null ? code : "",
					label != null ? label : "");

	}

	@JsonIgnore
	public boolean isBlank() {
		return StringUtils.isBlank(code)
			&& StringUtils.isBlank(label);

	}

	@Override
	public int hashCode() {
		return toComparableString().hashCode();
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;

		Qualifier other = (Qualifier) obj;

		return toComparableString().equals(other.toComparableString());
	}
}
