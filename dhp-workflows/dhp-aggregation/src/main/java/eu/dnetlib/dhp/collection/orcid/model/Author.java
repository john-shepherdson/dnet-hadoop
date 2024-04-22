
package eu.dnetlib.dhp.collection.orcid.model;

import java.util.ArrayList;
import java.util.List;

public class Author extends ORCIDItem {
	private String givenName;
	private String familyName;

	private String visibility;

	private String creditName;

	private List<String> otherNames;

	private List<Pid> otherPids;

	private String biography;

	private String lastModifiedDate;

	public String getBiography() {
		return biography;
	}

	public void setBiography(String biography) {
		this.biography = biography;
	}

	public String getGivenName() {
		return givenName;
	}

	public void setGivenName(String givenName) {
		this.givenName = givenName;
	}

	public String getFamilyName() {
		return familyName;
	}

	public void setFamilyName(String familyName) {
		this.familyName = familyName;
	}

	public String getCreditName() {
		return creditName;
	}

	public void setCreditName(String creditName) {
		this.creditName = creditName;
	}

	public List<String> getOtherNames() {
		return otherNames;
	}

	public void setOtherNames(List<String> otherNames) {
		this.otherNames = otherNames;
	}

	public String getVisibility() {
		return visibility;
	}

	public void setVisibility(String visibility) {
		this.visibility = visibility;
	}

	public List<Pid> getOtherPids() {
		return otherPids;
	}

	public void setOtherPids(List<Pid> otherPids) {
		this.otherPids = otherPids;
	}

	public String getLastModifiedDate() {
		return lastModifiedDate;
	}

	public void setLastModifiedDate(String lastModifiedDate) {
		this.lastModifiedDate = lastModifiedDate;
	}

	public void addOtherPid(final Pid pid) {

		if (otherPids == null)
			otherPids = new ArrayList<>();
		otherPids.add(pid);
	}
}
