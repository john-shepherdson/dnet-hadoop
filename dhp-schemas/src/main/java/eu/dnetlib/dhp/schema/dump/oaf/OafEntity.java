
package eu.dnetlib.dhp.schema.dump.oaf;

import eu.dnetlib.dhp.schema.dump.oaf.community.Project;

import java.io.Serializable;
import java.util.List;

public abstract class OafEntity extends Oaf implements Serializable {

	private String id;

	private List<String> originalId;

	private List<ControlledField> pid;

	private String dateofcollection;

	private List<Project> projects;

	public String getId() {
		return id;
	}

	public void setId(String id) {
		this.id = id;
	}

	public List<String> getOriginalId() {
		return originalId;
	}

	public void setOriginalId(List<String> originalId) {
		this.originalId = originalId;
	}

	public List<ControlledField> getPid() {
		return pid;
	}

	public void setPid(List<ControlledField> pid) {
		this.pid = pid;
	}

	public String getDateofcollection() {
		return dateofcollection;
	}

	public void setDateofcollection(String dateofcollection) {
		this.dateofcollection = dateofcollection;
	}

	public List<Project> getProjects() {
		return projects;
	}

	public void setProjects(List<Project> projects) {
		this.projects = projects;
	}

}
