
package eu.dnetlib.dhp.schema.orcid;

import java.io.Serializable;

public class Summary implements Serializable {
	private String creationMethod;
	private String completionDate;
	private String submissionDate;
	private String lastModifiedDate;
	private boolean claimed;
	private String deactivationDate;
	private boolean verifiedEmail;
	private boolean verifiedPrimaryEmail;

	public String getCreationMethod() {
		return creationMethod;
	}

	public void setCreationMethod(String creationMethod) {
		this.creationMethod = creationMethod;
	}

	public String getCompletionDate() {
		return completionDate;
	}

	public void setCompletionDate(String completionDate) {
		this.completionDate = completionDate;
	}

	public String getSubmissionDate() {
		return submissionDate;
	}

	public void setSubmissionDate(String submissionDate) {
		this.submissionDate = submissionDate;
	}

	public String getLastModifiedDate() {
		return lastModifiedDate;
	}

	public void setLastModifiedDate(String lastModifiedDate) {
		this.lastModifiedDate = lastModifiedDate;
	}

	public boolean isClaimed() {
		return claimed;
	}

	public void setClaimed(boolean claimed) {
		this.claimed = claimed;
	}

	public String getDeactivationDate() {
		return deactivationDate;
	}

	public void setDeactivationDate(String deactivationDate) {
		this.deactivationDate = deactivationDate;
	}

	public boolean isVerifiedEmail() {
		return verifiedEmail;
	}

	public void setVerifiedEmail(boolean verifiedEmail) {
		this.verifiedEmail = verifiedEmail;
	}

	public boolean isVerifiedPrimaryEmail() {
		return verifiedPrimaryEmail;
	}

	public void setVerifiedPrimaryEmail(boolean verifiedPrimaryEmail) {
		this.verifiedPrimaryEmail = verifiedPrimaryEmail;
	}
}
