
package eu.dnetlib.dhp.schema.dump.oaf;

import java.io.Serializable;
import java.util.List;

public class Author implements Serializable {

	private String fullname;

	private String name;

	private String surname;

	private Integer rank;

	private Pid pid;

	private List<String> affiliation;

	public String getFullname() {
		return fullname;
	}

	public void setFullname(String fullname) {
		this.fullname = fullname;
	}

	public String getName() {
		return name;
	}

	public void setName(String name) {
		this.name = name;
	}

	public String getSurname() {
		return surname;
	}

	public void setSurname(String surname) {
		this.surname = surname;
	}

	public Integer getRank() {
		return rank;
	}

	public void setRank(Integer rank) {
		this.rank = rank;
	}

	public Pid getPid() {
		return pid;
	}

	public void setPid(Pid pid) {
		this.pid = pid;
	}

	public List<String> getAffiliation() {
		return affiliation;
	}

	public void setAffiliation(List<String> affiliation) {
		this.affiliation = affiliation;
	}

}
