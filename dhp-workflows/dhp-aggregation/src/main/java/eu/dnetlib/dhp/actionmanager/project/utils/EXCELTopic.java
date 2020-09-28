
package eu.dnetlib.dhp.actionmanager.project.utils;

import java.io.Serializable;

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
	private String subjects;
	private String legalBasis;
	private String call;

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

	public String getSubjects() {
		return subjects;
	}

	public void setSubjects(String subjects) {
		this.subjects = subjects;
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
