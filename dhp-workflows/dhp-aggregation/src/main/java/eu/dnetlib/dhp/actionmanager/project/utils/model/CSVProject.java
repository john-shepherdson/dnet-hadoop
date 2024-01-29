
package eu.dnetlib.dhp.actionmanager.project.utils.model;

import java.io.Serializable;

import com.opencsv.bean.CsvBindByName;

/**
 * the mmodel for the projects csv file
 */
public class CSVProject implements Serializable {

	@CsvBindByName(column = "id")
	private String id;

	@CsvBindByName(column = "legalBasis")
	private String programme;

	@CsvBindByName(column = "topics")
	private String topics;

	public String getId() {
		return id;
	}

	public void setId(String id) {
		this.id = id;
	}

	public String getProgramme() {
		return programme;
	}

	public void setProgramme(String programme) {
		this.programme = programme;
	}

	public String getTopics() {
		return topics;
	}

	public void setTopics(String topics) {
		this.topics = topics;
	}

}
