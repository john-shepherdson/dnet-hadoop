
package eu.dnetlib.dhp.schema.dump.oaf.graph;

import java.io.Serializable;

/**
 * To represent entity of type RC/RI. It has the following parameters, which are mostly derived by the profile
 * - private String id to store the openaire id for the entity. Is has as code 00 and will be created as
 *   00|context_____::md5(originalId)
 * 	private String originalId to store the id of the context as provided in the profile (i.e. mes)
 * 	private String name to store the name of the context (got from the label attribute in the context definition)
 * 	private String type to store the type of the context (i.e.: research initiative or research community)
 * 	private String description to store the description of the context as given in the profile
 * 	private String zenodo_community to store the zenodo community associated to the context (main zenodo community)
 */
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
