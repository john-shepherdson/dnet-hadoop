/**
 * Deserialization of the information in the context needed to create Context Entities, and relations between
 * context entities and datasources and projects
 */

package eu.dnetlib.dhp.oa.graph.dump.graph;

import java.io.Serializable;
import java.util.List;

public class ContextInfo implements Serializable {
	private String id;
	private String description;
	private String type;
	private String zenodocommunity;
	private String name;
	private List<String> projectList;
	private List<String> datasourceList;
	private List<String> subject;

	public List<String> getSubject() {
		return subject;
	}

	public void setSubject(List<String> subject) {
		this.subject = subject;
	}

	public String getName() {
		return name;
	}

	public void setName(String name) {
		this.name = name;
	}

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

	public String getType() {
		return type;
	}

	public void setType(String type) {
		this.type = type;
	}

	public String getZenodocommunity() {
		return zenodocommunity;
	}

	public void setZenodocommunity(String zenodocommunity) {
		this.zenodocommunity = zenodocommunity;
	}

	public List<String> getProjectList() {
		return projectList;
	}

	public void setProjectList(List<String> projectList) {
		this.projectList = projectList;
	}

	public List<String> getDatasourceList() {
		return datasourceList;
	}

	public void setDatasourceList(List<String> datasourceList) {
		this.datasourceList = datasourceList;
	}
}
