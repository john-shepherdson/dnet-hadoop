
package eu.dnetlib.dhp.schema.dump.oaf.graph;

import java.io.Serializable;

public class ResearchInitiative implements Serializable {
	private String id; // openaireId
	private String originalId; // context id
	private String name; // context name
	private String type; // context type: research initiative or research community
	private String description;
	private String zenodo_community;

	public String getZenodo_community() {
		return zenodo_community;
	}

	public void setZenodo_community(String zenodo_community) {
		this.zenodo_community = zenodo_community;
	}

	public String getType() {
		return type;
	}

	public void setType(String type) {
		this.type = type;
	}

	public String getId() {
		return id;
	}

	public void setId(String id) {
		this.id = id;
	}

	public String getName() {
		return name;
	}

	public void setName(String label) {
		this.name = label;
	}

	public String getOriginalId() {
		return originalId;
	}

	public void setOriginalId(String originalId) {
		this.originalId = originalId;
	}

	public String getDescription() {
		return description;
	}

	public void setDescription(String description) {
		this.description = description;
	}
}
