
package eu.dnetlib.dhp.schema.oaf;

import java.io.Serializable;
import java.util.Objects;

/**
 * To store information about the ec programme for the project. It has the following parameters:
 * - private String code to store the code of the programme
 * - private String description to store the description of the programme
 */


public class H2020Programme implements Serializable {
	private String code;
	private String description;

	public String getCode() {
		return code;
	}

	public void setCode(String code) {
		this.code = code;
	}

	public String getDescription() {
		return description;
	}

	public void setDescription(String description) {
		this.description = description;
	}

	@Override
	public boolean equals(Object o) {
		if (this == o)
			return true;
		if (o == null || getClass() != o.getClass())
			return false;

		H2020Programme h2020Programme = (H2020Programme) o;
		return Objects.equals(code, h2020Programme.code);
	}

}
