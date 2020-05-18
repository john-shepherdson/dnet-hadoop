
package eu.dnetlib.dhp.actionmanager.project;

import java.io.Serializable;

public class Programme implements Serializable {
	private String rcn;
	private String code;
	private String title;
	private String shortTitle;
	private String language;

	public String getRcn() {
		return rcn;
	}

	public void setRcn(String rcn) {
		this.rcn = rcn;
	}

	public String getCode() {
		return code;
	}

	public void setCode(String code) {
		this.code = code;
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

	public String getLanguage() {
		return language;
	}

	public void setLanguage(String language) {
		this.language = language;
	}
}
