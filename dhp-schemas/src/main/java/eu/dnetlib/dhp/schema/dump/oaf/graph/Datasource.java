
package eu.dnetlib.dhp.schema.dump.oaf.graph;

import java.io.Serializable;
import java.util.List;

import eu.dnetlib.dhp.schema.dump.oaf.Container;
import eu.dnetlib.dhp.schema.dump.oaf.ControlledField;
import eu.dnetlib.dhp.schema.dump.oaf.KeyValue;

/**
 * To store information about the datasource OpenAIRE collects information from. It contains the following parameters: -
 * id of type String to store the OpenAIRE id for the datasource. It corresponds to the parameter id of the datasource
 * represented in the internal model - originalId of type List<String> to store the list of original ids associated to
 * the datasource. It corresponds to the parameter originalId of the datasource represented in the internal model. The
 * null values are filtered out - pid of type List<eu.dnetlib.shp.schema.dump.oaf.ControlledField> to store the
 * persistent identifiers for the datasource. For each pid in the datasource represented in the internal model one pid
 * in the external model is produced as : - schema corresponds to pid.qualifier.classid of the datasource represented in
 * the internal model - value corresponds to pid.value of the datasource represented in the internal model -
 * datasourceType of type eu.dnetlib.dhp.schema.dump.oaf.ControlledField to store the datasource type (e.g.
 * pubsrepository::institutional, Institutional Repository) as in the dnet vocabulary dnet:datasource_typologies. It
 * corresponds to datasourcetype of the datasource represented in the internal model and : - code corresponds to
 * datasourcetype.classid - value corresponds to datasourcetype.classname - openairecompatibility of type String to
 * store information about the OpenAIRE compatibility of the ingested results (which guidelines they are compliant to).
 * It corresponds to openairecompatibility.classname of the datasource represented in the internal model - officialname
 * of type Sgtring to store the official name of the datasource. It correspond to officialname.value of the datasource
 * represented in the internal model - englishname of type String to store the English name of the datasource. It
 * corresponds to englishname.value of the datasource represented in the internal model - websiteurl of type String to
 * store the URL of the website of the datasource. It corresponds to websiteurl.value of the datasource represented in
 * the internal model - logourl of type String to store the URL of the logo for the datasource. It corresponds to
 * logourl.value of the datasource represented in the internal model - dateofvalidation of type String to store the data
 * of validation against the guidelines for the datasource records. It corresponds to dateofvalidation.value of the
 * datasource represented in the internal model - description of type String to store the description for the
 * datasource. It corresponds to description.value of the datasource represented in the internal model
 */
public class Datasource implements Serializable {

	private String id; // string

	private List<String> originalId; // list string

	private List<ControlledField> pid; // list<String>

	private ControlledField datasourcetype; // value

	private String openairecompatibility; // value

	private String officialname; // string

	private String englishname; // string

	private String websiteurl; // string

	private String logourl; // string

	private String dateofvalidation; // string

	private String description; // description

	private List<String> subjects; // List<String>

	// opendoar specific fields (od*)

	private List<String> languages; // odlanguages List<String>

	private List<String> contenttypes; // odcontent types List<String>

	// re3data fields
	private String releasestartdate; // string

	private String releaseenddate; // string

	private String missionstatementurl; // string

	// {open, restricted or closed}
	private String accessrights; // databaseaccesstype string

	// {open, restricted or closed}
	private String uploadrights; // datauploadtype string

	// {feeRequired, registration, other}
	private String databaseaccessrestriction; // string

	// {feeRequired, registration, other}
	private String datauploadrestriction; // string

	private Boolean versioning; // boolean

	private String citationguidelineurl; // string

	// {yes, no, uknown}

	private String pidsystems; // string

	private String certificates; // string

	private List<Object> policies; //

	private Container journal; // issn etc del Journal

	public String getId() {
		return id;
	}

	public void setId(String id) {
		this.id = id;
	}

	public List<String> getOriginalId() {
		return originalId;
	}

	public void setOriginalId(List<String> originalId) {
		this.originalId = originalId;
	}

	public List<ControlledField> getPid() {
		return pid;
	}

	public void setPid(List<ControlledField> pid) {
		this.pid = pid;
	}

	public ControlledField getDatasourcetype() {
		return datasourcetype;
	}

	public void setDatasourcetype(ControlledField datasourcetype) {
		this.datasourcetype = datasourcetype;
	}

	public String getOpenairecompatibility() {
		return openairecompatibility;
	}

	public void setOpenairecompatibility(String openairecompatibility) {
		this.openairecompatibility = openairecompatibility;
	}

	public String getOfficialname() {
		return officialname;
	}

	public void setOfficialname(String officialname) {
		this.officialname = officialname;
	}

	public String getEnglishname() {
		return englishname;
	}

	public void setEnglishname(String englishname) {
		this.englishname = englishname;
	}

	public String getWebsiteurl() {
		return websiteurl;
	}

	public void setWebsiteurl(String websiteurl) {
		this.websiteurl = websiteurl;
	}

	public String getLogourl() {
		return logourl;
	}

	public void setLogourl(String logourl) {
		this.logourl = logourl;
	}

	public String getDateofvalidation() {
		return dateofvalidation;
	}

	public void setDateofvalidation(String dateofvalidation) {
		this.dateofvalidation = dateofvalidation;
	}

	public String getDescription() {
		return description;
	}

	public void setDescription(String description) {
		this.description = description;
	}

	public List<String> getSubjects() {
		return subjects;
	}

	public void setSubjects(List<String> subjects) {
		this.subjects = subjects;
	}

	public List<String> getLanguages() {
		return languages;
	}

	public void setLanguages(List<String> languages) {
		this.languages = languages;
	}

	public List<String> getContenttypes() {
		return contenttypes;
	}

	public void setContenttypes(List<String> contenttypes) {
		this.contenttypes = contenttypes;
	}

	public String getReleasestartdate() {
		return releasestartdate;
	}

	public void setReleasestartdate(String releasestartdate) {
		this.releasestartdate = releasestartdate;
	}

	public String getReleaseenddate() {
		return releaseenddate;
	}

	public void setReleaseenddate(String releaseenddate) {
		this.releaseenddate = releaseenddate;
	}

	public String getMissionstatementurl() {
		return missionstatementurl;
	}

	public void setMissionstatementurl(String missionstatementurl) {
		this.missionstatementurl = missionstatementurl;
	}

	public String getAccessrights() {
		return accessrights;
	}

	public void setAccessrights(String accessrights) {
		this.accessrights = accessrights;
	}

	public String getUploadrights() {
		return uploadrights;
	}

	public void setUploadrights(String uploadrights) {
		this.uploadrights = uploadrights;
	}

	public String getDatabaseaccessrestriction() {
		return databaseaccessrestriction;
	}

	public void setDatabaseaccessrestriction(String databaseaccessrestriction) {
		this.databaseaccessrestriction = databaseaccessrestriction;
	}

	public String getDatauploadrestriction() {
		return datauploadrestriction;
	}

	public void setDatauploadrestriction(String datauploadrestriction) {
		this.datauploadrestriction = datauploadrestriction;
	}

	public Boolean getVersioning() {
		return versioning;
	}

	public void setVersioning(Boolean versioning) {
		this.versioning = versioning;
	}

	public String getCitationguidelineurl() {
		return citationguidelineurl;
	}

	public void setCitationguidelineurl(String citationguidelineurl) {
		this.citationguidelineurl = citationguidelineurl;
	}

	public String getPidsystems() {
		return pidsystems;
	}

	public void setPidsystems(String pidsystems) {
		this.pidsystems = pidsystems;
	}

	public String getCertificates() {
		return certificates;
	}

	public void setCertificates(String certificates) {
		this.certificates = certificates;
	}

	public List<Object> getPolicies() {
		return policies;
	}

	public void setPolicies(List<Object> policiesr3) {
		this.policies = policiesr3;
	}

	public Container getJournal() {
		return journal;
	}

	public void setJournal(Container journal) {
		this.journal = journal;
	}
}
