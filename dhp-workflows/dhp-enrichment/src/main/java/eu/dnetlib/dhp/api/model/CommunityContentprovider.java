package eu.dnetlib.dhp.api.model;

import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.google.gson.Gson;
import eu.dnetlib.dhp.bulktag.community.SelectionConstraints;


@JsonAutoDetect
@JsonIgnoreProperties(ignoreUnknown = true)
public class CommunityContentprovider {
	private String openaireId;
	private SelectionConstraints selectioncriteria;

	private String enabled;

	public String getEnabled() {
		return enabled;
	}

	public void setEnabled(String enabled) {
		this.enabled = enabled;
	}

	public String getOpenaireId() {
		return openaireId;
	}

	public void setOpenaireId(final String openaireId) {
		this.openaireId = openaireId;
	}


	public SelectionConstraints getSelectioncriteria() {

		return  this.selectioncriteria;
	}

	public void setSelectioncriteria(SelectionConstraints selectioncriteria) {
		this.selectioncriteria = selectioncriteria;

	}
}
