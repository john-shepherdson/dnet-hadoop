
package eu.dnetlib.dhp.schema.oaf;

import java.io.Serializable;
import java.util.*;

public class Author implements Serializable {

	private String fullname;

	private String name;

	private String surname;

	// START WITH 1
	private Integer rank;

	private List<StructuredProperty> pid;

	private List<Field<String>> affiliation;

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

	public List<StructuredProperty> getPid() {
		return pid;
	}

	public void setPid(List<StructuredProperty> pid) {
		this.pid = pid;
	}

	public List<Field<String>> getAffiliation() {
		return affiliation;
	}

	public void setAffiliation(List<Field<String>> affiliation) {
		this.affiliation = affiliation;
	}

	@Override
	public boolean equals(Object o) {
		if (this == o)
			return true;
		if (o == null || getClass() != o.getClass())
			return false;
		Author author = (Author) o;
		return Objects.equals(fullname, author.fullname)
			&& Objects.equals(name, author.name)
			&& Objects.equals(surname, author.surname)
			&& Objects.equals(rank, author.rank)
			&& Objects.equals(pid, author.pid)
			&& Objects.equals(affiliation, author.affiliation);
	}

	@Override
	public int hashCode() {
		return Objects.hash(fullname, name, surname, rank, pid, affiliation);
	}

}
