
package eu.dnetlib.dhp.schema.dump.oaf.graph;

import java.io.Serializable;
import java.util.List;

import eu.dnetlib.dhp.schema.dump.oaf.KeyValue;

//At the moment the funder is map in the project.
//We will reserve to decide if to create specific entities for funders or let them in the project
public class Project implements Serializable {
	private String id;
	// private List<KeyValue> collectedfrom;
	private String websiteurl;
	private String code;
	private String acronym;
	private String title;
	private String startdate;

	private String enddate;

	private String callidentifier;

	private String keywords;

	private String duration;

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

	public String getDuration() {
		return duration;
	}

	public void setDuration(String duration) {
		this.duration = duration;
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

//	public List<KeyValue> getCollectedfrom() {
//		return collectedfrom;
//	}
//
//	public void setCollectedfrom(List<KeyValue> collectedfrom) {
//		this.collectedfrom = collectedfrom;
//	}
}
