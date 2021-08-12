
package eu.dnetlib.dhp.common.collection.models;

import java.io.Serializable;

import com.opencsv.bean.CsvBindByName;
import com.opencsv.bean.CsvIgnore;

/**
 * The model for the programme csv file
 */
public class CSVProgramme implements Serializable {

	@CsvBindByName(column = "code")
	private String code;

	@CsvBindByName(column = "title")
	private String title;

	@CsvBindByName(column = "shortTitle")
	private String shortTitle;

	@CsvBindByName(column = "language")
	private String language;

	@CsvIgnore
	private String classification;

	@CsvIgnore
	private String classification_short;

	public String getClassification_short() {
		return classification_short;
	}

	public void setClassification_short(String classification_short) {
		this.classification_short = classification_short;
	}

	public String getClassification() {
		return classification;
	}

	public void setClassification(String classification) {
		this.classification = classification;
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
