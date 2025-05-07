
package eu.dnetlib.dhp.person;

import java.io.Serializable;
import java.util.List;

import eu.dnetlib.dhp.schema.oaf.Author;
import eu.dnetlib.dhp.schema.oaf.Measure;
import eu.dnetlib.dhp.schema.oaf.Result;

public class ResultSubset implements Serializable {
	private String id;
	private List<Author> author;
	private List<Measure> measures;

	public String getId() {
		return id;
	}

	public void setId(String id) {
		this.id = id;
	}

	public List<Author> getAuthor() {
		return author;
	}

	public void setAuthor(List<Author> author) {
		this.author = author;
	}

	public List<Measure> getMeasures() {
		return measures;
	}

	public void setMeasures(List<Measure> measures) {
		this.measures = measures;
	}

	public static ResultSubset newInstance(Result r) {
		ResultSubset rs = new ResultSubset();
		rs.id = r.getId();
		rs.author = r.getAuthor();
		rs.measures = r.getMeasures();
		return rs;
	}
}
