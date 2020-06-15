
package eu.dnetlib.dhp.schema.dump.oaf;

import java.util.List;

import eu.dnetlib.dhp.schema.oaf.Project;

public class Projects {

	private String id;// OpenAIRE id
	private String code;

	private String acronym;

	private String title;

	private Funder funder;

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

	public static Projects newInstance(String id, String code, String acronym, String title, Funder funder) {
		Projects projects = new Projects();
		projects.setAcronym(acronym);
		projects.setCode(code);
		projects.setFunder(funder);
		projects.setId(id);
		projects.setTitle(title);
		return projects;
	}
}
