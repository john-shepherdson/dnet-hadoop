
package eu.dnetlib.doiboost.orcidnodoi.model;

import java.io.Serializable;

import eu.dnetlib.doiboost.orcid.model.AuthorData;

public class Contributor extends AuthorData implements Serializable {
	private String sequence;
	private String role;
	private boolean simpleMatch = false;
	private Double score = 0.0;
	private boolean bestMatch = false;

	public String getSequence() {
		return sequence;
	}

	public void setSequence(String sequence) {
		this.sequence = sequence;
	}

	public String getRole() {
		return role;
	}

	public void setRole(String role) {
		this.role = role;
	}

	public boolean isSimpleMatch() {
		return simpleMatch;
	}

	public void setSimpleMatch(boolean simpleMatch) {
		this.simpleMatch = simpleMatch;
	}

	public Double getScore() {
		return score;
	}

	public void setScore(Double score) {
		this.score = score;
	}

	public boolean isBestMatch() {
		return bestMatch;
	}

	public void setBestMatch(boolean bestMatch) {
		this.bestMatch = bestMatch;
	}
}
