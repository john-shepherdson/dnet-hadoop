
package eu.dnetlib.doiboost.orcidnodoi.model;

/**
 * This class models the data related to external id, that are retrieved from an orcid publication
 */

public class ExternalId {
	private String type;
	private String value;
	private String relationShip;

	public String getType() {
		return type;
	}

	public void setType(String type) {
		this.type = type;
	}

	public String getValue() {
		return value;
	}

	public void setValue(String value) {
		this.value = value;
	}

	public String getRelationShip() {
		return relationShip;
	}

	public void setRelationShip(String relationShip) {
		this.relationShip = relationShip;
	}
}
