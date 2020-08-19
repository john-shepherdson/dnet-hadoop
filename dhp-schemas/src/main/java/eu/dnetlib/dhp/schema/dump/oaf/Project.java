
package eu.dnetlib.dhp.schema.dump.oaf;

import java.io.Serializable;

/**
 * This class to store the common information about the project that will be dumped for community and for the whole graph
 * - private String id to store the id of the project (OpenAIRE id)
 * - private String code to store the grant agreement of the project
 * - private String acronym to store the acronym of the project
 * - private String title to store the tile of the project
 */
public class Project implements Serializable {
	protected String id;// OpenAIRE id
	protected String code;

	protected String acronym;

	protected String title;

	public String getId() {
		return id;
	}

	public void setId(String id) {
		this.id = id;
	}

	public String getCode() {
		return code;
	}

	public void setCode(String code) {
		this.code = code;
	}

	public String getAcronym() {
		return acronym;
	}

	public void setAcronym(String acronym) {
		this.acronym = acronym;
	}

	public String getTitle() {
		return title;
	}

	public void setTitle(String title) {
		this.title = title;
	}
}
