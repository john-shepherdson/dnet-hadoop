
package eu.dnetlib.dhp.actionmanager.project.utils.model;

import java.io.Serializable;

/**
 * the model class for the topic excel file
 */
@Deprecated
public class EXCELTopic implements Serializable {
	private String rcn;
	private String language;
	private String code;
	private String parentProgramme;
	private String frameworkProgramme;
	private String startDate;
	private String endDate;
	private String title;
	private String shortTitle;
	private String objective;
	private String keywords;
	private String legalBasis;
	private String call;
	private String id;
	private String contentUpdateDate;

	public String getContentUpdateDate() {
		return contentUpdateDate;
	}

	public void setContentUpdateDate(String contentUpdateDate) {
		this.contentUpdateDate = contentUpdateDate;
	}

	public String getId() {
		return id;
	}

	public void setId(String id) {
		this.id = id;
	}

	public String getRcn() {
		return rcn;
	}

	public void setRcn(String rcn) {
		this.rcn = rcn;
	}

	public String getLanguage() {
		return language;
	}

	public void setLanguage(String language) {
		this.language = language;
	}

	public String getCode() {
		return code;
	}

	public void setCode(String code) {
		this.code = code;
	}

	public String getParentProgramme() {
		return parentProgramme;
	}

	public void setParentProgramme(String parentProgramme) {
		this.parentProgramme = parentProgramme;
	}

	public String getFrameworkProgramme() {
		return frameworkProgramme;
	}

	public void setFrameworkProgramme(String frameworkProgramme) {
		this.frameworkProgramme = frameworkProgramme;
	}

	public String getStartDate() {
		return startDate;
	}

	public void setStartDate(String startDate) {
		this.startDate = startDate;
	}

	public String getEndDate() {
		return endDate;
	}

	public void setEndDate(String endDate) {
		this.endDate = endDate;
	}

	public String getTitle() {
		return title;
	}

	public void setTitle(String title) {
		this.title = title;
	}

	public String getShortTitle() {
		return shortTitle;
	}

	public void setShortTitle(String shortTitle) {
		this.shortTitle = shortTitle;
	}

	public String getObjective() {
		return objective;
	}

	public void setObjective(String objective) {
		this.objective = objective;
	}

	public String getKeywords() {
		return keywords;
	}

	public void setKeywords(String keywords) {
		this.keywords = keywords;
	}

	public String getLegalBasis() {
		return legalBasis;
	}

	public void setLegalBasis(String legalBasis) {
		this.legalBasis = legalBasis;
	}

	public String getCall() {
		return call;
	}

	public void setCall(String call) {
		this.call = call;
	}
}
