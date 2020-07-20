
package eu.dnetlib.dhp.schema.dump.oaf.community;

import eu.dnetlib.dhp.schema.dump.oaf.Funder;
import eu.dnetlib.dhp.schema.dump.oaf.Provenance;

import java.io.Serializable;

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
