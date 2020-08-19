
package eu.dnetlib.dhp.schema.dump.oaf.community;

import java.io.Serializable;

import eu.dnetlib.dhp.schema.dump.oaf.Provenance;

/**
 * To store information about the project related to the result. This information is not directly mapped from the result
 * represented in the internal model because it is not there. The mapped result will be enriched with project
 * information derived by relation between results and projects.
 * Project extends eu.dnetlib.dhp.schema.dump.oaf.Project with the following parameters:
 * - funder of
 * type eu.dnetlib.dhp.schema.dump.oaf.community.Funder to store information about the funder funding the project -
 * provenance of type eu.dnetlib.dhp.schema.dump.oaf.Provenance to store information about the. provenance of the
 * association between the result and the project
 */
public class Project extends eu.dnetlib.dhp.schema.dump.oaf.Project {

	private Funder funder;

	private Provenance provenance;

	public Provenance getProvenance() {
		return provenance;
	}

	public void setProvenance(Provenance provenance) {
		this.provenance = provenance;
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
