
package eu.dnetlib.dhp.oa.graph.hostedbymap.model.doaj;

import java.io.Serializable;

public class Ref implements Serializable {
	private String aims_scope;
	private String journal;
	private String oa_statement;
	private String author_instructions;
	private String license_terms;

	public String getAims_scope() {
		return aims_scope;
	}

	public void setAims_scope(String aims_scope) {
		this.aims_scope = aims_scope;
	}

	public String getJournal() {
		return journal;
	}

	public void setJournal(String journal) {
		this.journal = journal;
	}

	public String getOa_statement() {
		return oa_statement;
	}

	public void setOa_statement(String oa_statement) {
		this.oa_statement = oa_statement;
	}

	public String getAuthor_instructions() {
		return author_instructions;
	}

	public void setAuthor_instructions(String author_instructions) {
		this.author_instructions = author_instructions;
	}

	public String getLicense_terms() {
		return license_terms;
	}

	public void setLicense_terms(String license_terms) {
		this.license_terms = license_terms;
	}
}
