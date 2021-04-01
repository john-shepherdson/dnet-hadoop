
package eu.dnetlib.dhp.schema.orcid;

import java.io.Serializable;

/**
 * This class models the data related to a publication date, that are retrieved from an orcid publication
 */

public class PublicationDate implements Serializable {
	private String year;
	private String month;
	private String day;

	public String getYear() {
		return year;
	}

	public void setYear(String year) {
		this.year = year;
	}

	public String getMonth() {
		return month;
	}

	public void setMonth(String month) {
		this.month = month;
	}

	public String getDay() {
		return day;
	}

	public void setDay(String day) {
		this.day = day;
	}
}
