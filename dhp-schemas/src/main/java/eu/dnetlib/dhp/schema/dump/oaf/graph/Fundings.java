
package eu.dnetlib.dhp.schema.dump.oaf.graph;

import java.io.Serializable;

/**
 * To store inforamtion about the funding stream. It has two parameters:
 * - private String id to store the id of the fundings stream. The id is created by appending the shortname of the
 *   funder to the name of each level in the xml representing the fundng stream. For example: if the funder is the
 *   European Commission, the funding level 0 name is FP7, the funding level 1 name is SP3 and the funding level 2 name is
 *   PEOPLE then the id will be: EC::FP7::SP3::PEOPLE
 * - private String description to describe the funding stream. It is created by concatenating the description of each funding
 * 	 level so for the example above the description would be: SEVENTH FRAMEWORK PROGRAMME - SP3-People - Marie-Curie Actions
 */
public class Fundings implements Serializable {

	private String id;
	private String description;

	public String getId() {
		return id;
	}

	public void setId(String id) {
		this.id = id;
	}

	public String getDescription() {
		return description;
	}

	public void setDescription(String description) {
		this.description = description;
	}
}
