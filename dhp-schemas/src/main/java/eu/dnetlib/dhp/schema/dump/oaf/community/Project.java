
package eu.dnetlib.dhp.schema.dump.oaf.community;

import java.io.Serializable;

import eu.dnetlib.dhp.schema.dump.oaf.Provenance;

/**
 * to store information about the project related to the result. This information is not directly mapped from the result represented in the internal model because it is not there. The mapped result will be enriched with project information derived by relation between results and projects. The way used to do it will be described afterwards. Project class has the following parameters:
 *     - id of type String to store the OpenAIRE id for the Project
 *     - code of type String to store the grant agreement
 *     - acronym of type String to store the acronym for the project
 *     - title of type String to store the title of the project
 *     - funder of type eu.dnetlib.dhp.schema.dump.oaf.community.Funder to store information about the funder funding the project
 *     - provenance of type eu.dnetlib.dhp.schema.dump.oaf.Provenance to store information about the. provenance of the association between the result and the project
 */
public class Project implements Serializable {

	private String id;// OpenAIRE id
	private String code;

	private String acronym;

	private String title;

	private Funder funder;

	private Provenance provenance;

	public Provenance getProvenance() {
		return provenance;
	}

	public void setProvenance(Provenance provenance) {
		this.provenance = provenance;
	}

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

	public Funder getFunder() {
		return funder;
	}

	public void setFunder(Funder funders) {
		this.funder = funders;
	}

	public static Project newInstance(String id, String code, String acronym, String title, Funder funder) {
		Project project = new Project();
		project.setAcronym(acronym);
		project.setCode(code);
		project.setFunder(funder);
		project.setId(id);
		project.setTitle(title);
		return project;
	}
}
