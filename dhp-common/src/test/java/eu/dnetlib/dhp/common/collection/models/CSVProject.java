
package eu.dnetlib.dhp.common.collection.models;

import java.io.Serializable;

import com.opencsv.bean.CsvBindByName;

/**
 * the mmodel for the projects csv file
 */
public class CSVProject implements Serializable {

	@CsvBindByName(column = "id")
	private String id;

	@CsvBindByName(column = "status")
	private String status;

	@CsvBindByName(column = "programme")
	private String programme;

	@CsvBindByName(column = "topics")
	private String topics;

	@CsvBindByName(column = "title")
	private String title;

	@CsvBindByName(column = "call")
	private String call;

	@CsvBindByName(column = "subjects")
	private String subjects;

	public String getId() {
		return id;
	}

	public void setId(String id) {
		this.id = id;
	}

	public String getStatus() {
		return status;
	}

	public void setStatus(String status) {
		this.status = status;
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

	public String getTitle() {
		return title;
	}

	public void setTitle(String title) {
		this.title = title;
	}

	public String getCall() {
		return call;
	}

	public void setCall(String call) {
		this.call = call;
	}

	public String getSubjects() {
		return subjects;
	}

	public void setSubjects(String subjects) {
		this.subjects = subjects;
	}

}
