
package eu.dnetlib.dhp.actionmanager.ror.model;

import java.io.Serializable;

public class GeonamesAdmin implements Serializable {

	private String asciiName;
	private Integer id;
	private String name;
	private String code;
	private final static long serialVersionUID = 7294958526269195673L;

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
