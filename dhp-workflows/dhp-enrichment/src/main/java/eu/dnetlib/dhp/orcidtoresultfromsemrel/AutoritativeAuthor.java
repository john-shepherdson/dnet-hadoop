
package eu.dnetlib.dhp.orcidtoresultfromsemrel;

public class AutoritativeAuthor {

	private String name;
	private String surname;
	private String fullname;
	private String orcid;

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

	public String getFullname() {
		return fullname;
	}

	public void setFullname(String fullname) {
		this.fullname = fullname;
	}

	public String getOrcid() {
		return orcid;
	}

	public void setOrcid(String orcid) {
		this.orcid = orcid;
	}

	public static AutoritativeAuthor newInstance(String name, String surname, String fullname, String orcid) {
		AutoritativeAuthor aa = new AutoritativeAuthor();
		aa.name = name;
		aa.surname = surname;
		aa.fullname = fullname;
		aa.orcid = orcid;
		return aa;
	}

}
