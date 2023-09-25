
package eu.dnetlib.dhp.actionmanager.project.utils.model;

import java.io.Serializable;

/**
 * @author miriam.baglioni
 * @Date 24/02/23
 */
public class Project implements Serializable {
	private String acronym;
	private String contentUpdateDate;
	private String ecMaxContribution;
	private String ecSignatureDate;
	private String endDate;
	private String frameworkProgramme;
	private String fundingScheme;
	private String grantDoi;
	private String id;
	private String legalBasis;
	private String masterCall;
	private String nature;
	private String objective;
	private String rcn;
	private String startDate;
	private String status;
	private String subCall;
	private String title;
	private String topics;
	private String totalCost;

	public String getAcronym() {
		return acronym;
	}

	public void setAcronym(String acronym) {
		this.acronym = acronym;
	}

	public String getContentUpdateDate() {
		return contentUpdateDate;
	}

	public void setContentUpdateDate(String contentUpdateDate) {
		this.contentUpdateDate = contentUpdateDate;
	}

	public String getEcMaxContribution() {
		return ecMaxContribution;
	}

	public void setEcMaxContribution(String ecMaxContribution) {
		this.ecMaxContribution = ecMaxContribution;
	}

	public String getEcSignatureDate() {
		return ecSignatureDate;
	}

	public void setEcSignatureDate(String ecSignatureDate) {
		this.ecSignatureDate = ecSignatureDate;
	}

	public String getEndDate() {
		return endDate;
	}

	public void setEndDate(String endDate) {
		this.endDate = endDate;
	}

	public String getFrameworkProgramme() {
		return frameworkProgramme;
	}

	public void setFrameworkProgramme(String frameworkProgramme) {
		this.frameworkProgramme = frameworkProgramme;
	}

	public String getFundingScheme() {
		return fundingScheme;
	}

	public void setFundingScheme(String fundingScheme) {
		this.fundingScheme = fundingScheme;
	}

	public String getGrantDoi() {
		return grantDoi;
	}

	public void setGrantDoi(String grantDoi) {
		this.grantDoi = grantDoi;
	}

	public String getId() {
		return id;
	}

	public void setId(String id) {
		this.id = id;
	}

	public String getLegalBasis() {
		return legalBasis;
	}

	public void setLegalBasis(String legalBasis) {
		this.legalBasis = legalBasis;
	}

	public String getMasterCall() {
		return masterCall;
	}

	public void setMasterCall(String masterCall) {
		this.masterCall = masterCall;
	}

	public String getNature() {
		return nature;
	}

	public void setNature(String nature) {
		this.nature = nature;
	}

	public String getObjective() {
		return objective;
	}

	public void setObjective(String objective) {
		this.objective = objective;
	}

	public String getRcn() {
		return rcn;
	}

	public void setRcn(String rcn) {
		this.rcn = rcn;
	}

	public String getStartDate() {
		return startDate;
	}

	public void setStartDate(String startDate) {
		this.startDate = startDate;
	}

	public String getStatus() {
		return status;
	}

	public void setStatus(String status) {
		this.status = status;
	}

	public String getSubCall() {
		return subCall;
	}

	public void setSubCall(String subCall) {
		this.subCall = subCall;
	}

	public String getTitle() {
		return title;
	}

	public void setTitle(String title) {
		this.title = title;
	}

	public String getTopics() {
		return topics;
	}

	public void setTopics(String topics) {
		this.topics = topics;
	}

	public String getTotalCost() {
		return totalCost;
	}

	public void setTotalCost(String totalCost) {
		this.totalCost = totalCost;
	}
}
