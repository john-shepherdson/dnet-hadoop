
package eu.dnetlib.dhp.actionmanager.ror.model;

import java.io.Serializable;

import com.fasterxml.jackson.annotation.JsonProperty;

public class ExternalIds implements Serializable {

	@JsonProperty("Wikidata")
	private ExternalIdType wikidata;

	@JsonProperty("OrgRef")
	private ExternalIdType orgRef;

	@JsonProperty("ISNI")
	private ExternalIdType isni;

	@JsonProperty("FundRef")
	private ExternalIdType fundRef;

	@JsonProperty("GRID")
	private GridType grid;

	private final static long serialVersionUID = 686536347353680869L;

	public ExternalIdType getWikidata() {
		return wikidata;
	}

	public void setWikidata(final ExternalIdType wikidata) {
		this.wikidata = wikidata;
	}

	public ExternalIdType getOrgRef() {
		return orgRef;
	}

	public void setOrgRef(final ExternalIdType orgRef) {
		this.orgRef = orgRef;
	}

	public ExternalIdType getIsni() {
		return isni;
	}

	public void setIsni(final ExternalIdType isni) {
		this.isni = isni;
	}

	public ExternalIdType getFundRef() {
		return fundRef;
	}

	public void setFundRef(final ExternalIdType fundRef) {
		this.fundRef = fundRef;
	}

	public GridType getGrid() {
		return grid;
	}

	public void setGrid(final GridType grid) {
		this.grid = grid;
	}

}
