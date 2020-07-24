
package eu.dnetlib.dhp.schema.dump.oaf.graph;

import java.io.Serializable;

public class Levels implements Serializable {
	private String level;
	private String id;
	private String description;
	private String name;

	public String getLevel() {
		return level;
	}

	public void setLevel(String level) {
		this.level = level;
	}

	public String getId() {
		return id;
	}

	public void setId(String il) {
		this.id = il;
	}

	public String getDescription() {
		return description;
	}

	public void setDescription(String description) {
		this.description = description;
	}

	public String getName() {
		return name;
	}

	public void setName(String name) {
		this.name = name;
	}
}
