
package eu.dnetlib.dhp.actionmanager.ror.model;

import java.io.Serializable;

import com.fasterxml.jackson.annotation.JsonProperty;

public class GeonamesAdmin implements Serializable {

	@JsonProperty("ascii_name")
	private String asciiName;

	@JsonProperty("id")
	private Integer id;

	@JsonProperty("name")
	private String name;

	@JsonProperty("code")
	private String code;

	private static final long serialVersionUID = 7294958526269195673L;

	public String getAsciiName() {
		return asciiName;
	}

	public void setAsciiName(final String asciiName) {
		this.asciiName = asciiName;
	}

	public Integer getId() {
		return id;
	}

	public void setId(final Integer id) {
		this.id = id;
	}

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
