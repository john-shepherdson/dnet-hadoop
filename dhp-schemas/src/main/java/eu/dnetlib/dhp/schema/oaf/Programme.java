
package eu.dnetlib.dhp.schema.oaf;

import java.io.Serializable;
import java.util.Objects;

public class Programme implements Serializable {
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

		Programme programme = (Programme) o;
		return Objects.equals(code, programme.code);
	}

}
