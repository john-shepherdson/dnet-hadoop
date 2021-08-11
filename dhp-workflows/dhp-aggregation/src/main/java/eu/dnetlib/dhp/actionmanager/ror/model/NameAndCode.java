
package eu.dnetlib.dhp.actionmanager.ror.model;

import java.io.Serializable;

import com.fasterxml.jackson.annotation.JsonProperty;

public class NameAndCode implements Serializable {

	private static final long serialVersionUID = 5459836979206140843L;

	@JsonProperty("name")
	private String name;

	@JsonProperty("code")
	private String code;

	public String getName() {
		return name;
	}

	public void setName(final String name) {
		this.name = name;
	}

	public String getCode() {
		return code;
	}

	public void setCode(final String code) {
		this.code = code;
	}

}
