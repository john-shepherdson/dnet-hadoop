
package eu.dnetlib.dhp.schema.orcid;

import java.io.Serializable;
import java.util.List;

import com.google.common.collect.Lists;

/**
 * This class models the data that are retrieved from orcid publication
 */

public class AuthorData implements Serializable {

	private String oid;
	private String name;
	private String surname;
	private String creditName;
	private String errorCode;
	private List<String> otherNames;

	public String getErrorCode() {
		return errorCode;
	}

	public void setErrorCode(String errorCode) {
		this.errorCode = errorCode;
	}

	public String getName() {
		return name;
	}

	public void setName(String name) {
		this.name = name;
	}

	public String getSurname() {
		return surname;
	}

	public void setSurname(String surname) {
		this.surname = surname;
	}

	public String getCreditName() {
		return creditName;
	}

	public void setCreditName(String creditName) {
		this.creditName = creditName;
	}

	public String getOid() {
		return oid;
	}

	public void setOid(String oid) {
		this.oid = oid;
	}

	public List<String> getOtherNames() {
		return otherNames;
	}

	public void setOtherNames(List<String> otherNames) {
		if (this.otherNames == null) {
			this.otherNames = Lists.newArrayList();
		}
		this.otherNames = otherNames;
	}
}
