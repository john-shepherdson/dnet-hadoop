
package eu.dnetlib.dhp.schema.dump.oaf.graph;

import java.io.Serializable;
import java.util.List;

import eu.dnetlib.dhp.schema.dump.oaf.KeyValue;

/**
 * This is the class representing the Project in the model used for the dumps of the whole graph. At the moment the dump
 * of the Projects differs from the other dumps because we do not create relations between Funders (Organization) and
 * Projects but we put the information about the Funder within the Project representation. We also removed the
 * collected from element from the Project. No relation between the Project and the Datasource entity from which it is
 * collected will be created. We will never create relations between Project and Datasource. In case some relation will
 * be extracted from the Project they will refer the Funder and will be of type ( organization -> funds -> project,
 * project -> isFundedBy -> organization) We also removed the duration parameter because the most of times it is set to
 * 0. It has the following parameters:
 * - private String id to store the id of the project (OpenAIRE id)
 * - private String websiteurl to store the websiteurl of the project
 * - private String code to store the grant agreement of the project
 * - private String acronym to store the acronym of the project
 * - private String title to store the tile of the project
 * - private String startdate to store the start date
 * - private String enddate to store the end date
 * - private String callidentifier to store the call indentifier
 * - private String keywords to store the keywords
 * - private boolean openaccessmandateforpublications to store if the project must accomplish to the open access mandate
 *   for publications. This value will be set to true if one of the field in the project represented in the internal model
 *   is set to true
 * - private boolean openaccessmandatefordataset to store if the project must accomplish to the open access mandate for
 * 	 dataset. It is set to the value in the corresponding filed of the project represented in the internal model
 * - private List<String> subject to store the list of subjects of the project
 * - private List<Funder> funding to store the list of funder of the project
 * - private String summary to store the summary of the project
 * - private Granted granted to store the granted amount
 * - private List<Programme> programme to store the list of programmes the project is related to
 */

public class Project implements Serializable {
	private String id;

	private String websiteurl;
	private String code;
	private String acronym;
	private String title;
	private String startdate;

	private String enddate;

	private String callidentifier;

	private String keywords;

	private boolean openaccessmandateforpublications;

	private boolean openaccessmandatefordataset;
	private List<String> subject;

	private List<Funder> funding;

	private String summary;

	private Granted granted;

	private List<Programme> programme;

	public String getId() {
		return id;
	}

	public void setId(String id) {
		this.id = id;
	}

	public String getWebsiteurl() {
		return websiteurl;
	}

	public void setWebsiteurl(String websiteurl) {
		this.websiteurl = websiteurl;
	}

	public String getCode() {
		return code;
	}

	public void setCode(String code) {
		this.code = code;
	}

	public String getAcronym() {
		return acronym;
	}

	public void setAcronym(String acronym) {
		this.acronym = acronym;
	}

	public String getTitle() {
		return title;
	}

	public void setTitle(String title) {
		this.title = title;
	}

	public String getStartdate() {
		return startdate;
	}

	public void setStartdate(String startdate) {
		this.startdate = startdate;
	}

	public String getEnddate() {
		return enddate;
	}

	public void setEnddate(String enddate) {
		this.enddate = enddate;
	}

	public String getCallidentifier() {
		return callidentifier;
	}

	public void setCallidentifier(String callidentifier) {
		this.callidentifier = callidentifier;
	}

	public String getKeywords() {
		return keywords;
	}

	public void setKeywords(String keywords) {
		this.keywords = keywords;
	}

	public boolean isOpenaccessmandateforpublications() {
		return openaccessmandateforpublications;
	}

	public void setOpenaccessmandateforpublications(boolean openaccessmandateforpublications) {
		this.openaccessmandateforpublications = openaccessmandateforpublications;
	}

	public boolean isOpenaccessmandatefordataset() {
		return openaccessmandatefordataset;
	}

	public void setOpenaccessmandatefordataset(boolean openaccessmandatefordataset) {
		this.openaccessmandatefordataset = openaccessmandatefordataset;
	}

	public List<String> getSubject() {
		return subject;
	}

	public void setSubject(List<String> subject) {
		this.subject = subject;
	}

	public List<Funder> getFunding() {
		return funding;
	}

	public void setFunding(List<Funder> funding) {
		this.funding = funding;
	}

	public String getSummary() {
		return summary;
	}

	public void setSummary(String summary) {
		this.summary = summary;
	}

	public Granted getGranted() {
		return granted;
	}

	public void setGranted(Granted granted) {
		this.granted = granted;
	}

	public List<Programme> getProgramme() {
		return programme;
	}

	public void setProgramme(List<Programme> programme) {
		this.programme = programme;
	}

}
