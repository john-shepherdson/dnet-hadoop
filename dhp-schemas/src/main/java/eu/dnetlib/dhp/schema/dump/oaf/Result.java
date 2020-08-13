
package eu.dnetlib.dhp.schema.dump.oaf;

import java.io.Serializable;
import java.util.List;

import eu.dnetlib.dhp.schema.dump.oaf.community.Project;

/**
 * To represent the dumped result. It will be extended in the dump for Research Communities -
 * Research Initiative/Infrastructures. It has the following parameters:
 * - author of type List<eu.dnetlib.dhpschema.dump.oaf.Author> to describe the authors of a result.
 *   For each author in the result represented in the internal model one author in the esternal model is produced.
 * - type of type String to represent the category of the result. Possible values are publication, dataset, software,
 *   other. It corresponds to resulttype.classname of the dumped result
 * - language of type eu.dnetlib.dhp.schema.dump.oaf.Qualifier to store information about the language of the result.
 *   It is dumped as
 *         - code corresponds to language.classid
 *         - value corresponds to language.classname
 * - country of type List<eu.dnetlib.dhp.schema.dump.oaf.Country> to store the country list to which the result is
 *   associated. For each country in the result respresented in the internal model one country in the external model
 *   is produces
 * - subjects of type List<eu.dnetlib.dhp.dump.oaf.Subject> to store the subjects for the result. For each subject in
 *   the result represented in the internal model one subject in the external model is produced
 * - maintitle of type String to store the main title of the result. It corresponds to the value of the first title in
 *   the resul to be dumped having classid equals to "main title"
 * - subtitle of type String to store the subtitle of the result. It corresponds to the value of the first title in the
 *   resul to be dumped having classid equals to "subtitle"
 * - description of type List<String> to store the description of the result. It corresponds to the list of
 *   description.value in the result represented in the internal model
 * - publicationdate of type String to store the pubblication date. It corresponds to dateofacceptance.value in the
 *   result represented in the internal model
 * - publisher of type String to store information about the publisher. It corresponds to publisher.value of the result
 *   represented in the intrenal model
 * - embargoenddate of type String to store the embargo end date. It corresponds to embargoenddate.value of the result
 *   represented in the internal model
 * - source of type List<String> See definition of Dublin Core field dc:source. It corresponds to the list of
 *   source.value in the result represented in the internal model
 * - format of type List<String> It corresponds to the list of format.value in the result represented in the internal model
 * - contributor of type List<String> to represent contributors for this result. It corresponds to the list of
 *   contributor.value in the result represented in the internal model
 * - coverage of type String. It corresponds to the list of coverage.value in the result represented in the internal model
 * - bestaccessright of type eu.dnetlib.dhp.schema.dump.oaf.AccessRight to store informatin about the openest access
 *   right associated to the manifestations of this research results. It corresponds to the same parameter in the result
 *   represented in the internal model
 * - instance of type List<eu.dnetlib.dhp.schema.dump.oaf.Instance> to store all the instances associated to the result.
 *   It corresponds to the same parameter in the result represented in the internal model
 * - container of type eu.dnetlib.dhp.schema/dump.oaf.Container (only for result of type publication). It corresponds
 *   to the parameter journal of the result represented in the internal model
 * - documentationUrl of type List<String> (only for results of type software) to store the URLs to the software
 *   documentation. It corresponds to the list of documentationUrl.value of the result represented in the internal model
 * - codeRepositoryUrl of type String (only for results of type software) to store the URL to the repository with the
 *   source code. It corresponds to codeRepositoryUrl.value of the result represented in the internal model
 * - programmingLanguage of type String (only for results of type software) to store the programming language. It
 *   corresponds to programmingLanguaga.classid of the result represented in the internal model
 * - contactperson of type List<String> (only for results of type other) to store the contact person for this result.
 *   It corresponds to the list of contactperson.value of the result represented in the internal model
 * - contactgroup of type List<String> (only for results of type other) to store the information for the contact group.
 *   It corresponds to the list of contactgroup.value of the result represented in the internal model
 * - tool of type List<String> (only fro results of type other) to store information about tool useful for the
 *   interpretation and/or re-used of the research product. It corresponds to the list of tool.value in the result
 *   represented in the internal modelt
 * - size of type String (only for results of type dataset) to store the size of the dataset. It corresponds to
 *   size.value in the result represented in the internal model
 * - version of type String (only for results of type dataset) to store the version. It corresponds to version.value of
 *   the result represented in the internal model
 * - geolocation fo type List<eu.dnetlib.dhp.schema.dump.oaf.GeoLocation> (only for results of type dataset) to store
 *   geolocation information. For each geolocation element in the result represented in the internal model a GeoLocation
 *   in the external model il produced
 * - id of type String to store the OpenAIRE id of the result. It corresponds to the id of the result represented in
 *   the internal model
 * - originalId of type List<String> to store the original ids of the result. It corresponds to the originalId of the
 *   result represented in the internal model
 * - pid of type List<eu.dnetlib.dhp.schema.dump.oaf.ControlledField> to store the persistent identifiers for the result.
 *   For each pid in the results represented in the internal model one pid in the external model is produced.
 *   The value correspondence is:
 *        - scheme corresponds to pid.qualifier.classid of the result represented in the internal model
 *        - value corresponds to the pid.value of the result represented in the internal model
 * - dateofcollection of type String to store information about the time OpenAIRE collected the record. It corresponds
 *   to dateofcollection of the result represented in the internal model
 * - lasteupdatetimestamp of type String to store the timestamp of the last update of the record. It corresponds to
 *   lastupdatetimestamp of the resord represented in the internal model
 */
public class Result implements Serializable {

	private List<Author> author;

	// resulttype allows subclassing results into publications | datasets | software
	private String type; // resulttype

	// common fields
	private Qualifier language;

	private List<Country> country;

	private List<Subject> subjects;

	private String maintitle;

	private String subtitle;

	private List<String> description;

	private String publicationdate; // dateofacceptance;

	private String publisher;

	private String embargoenddate;

	private List<String> source;

	private List<String> format;

	private List<String> contributor;

	private List<String> coverage;

	private AccessRight bestaccessright;

	private List<Instance> instance;

	private Container container;// Journal

	private List<String> documentationUrl; // software

	private String codeRepositoryUrl; // software

	private String programmingLanguage; // software

	private List<String> contactperson; // orp

	private List<String> contactgroup; // orp

	private List<String> tool; // orp

	private String size; // dataset

	private String version; // dataset

	private List<GeoLocation> geolocation; // dataset

	private String id;

	private List<String> originalId;

	private List<ControlledField> pid;

	private String dateofcollection;

	private Long lastupdatetimestamp;

	public Long getLastupdatetimestamp() {
		return lastupdatetimestamp;
	}

	public void setLastupdatetimestamp(Long lastupdatetimestamp) {
		this.lastupdatetimestamp = lastupdatetimestamp;
	}

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

	public String getDateofcollection() {
		return dateofcollection;
	}

	public void setDateofcollection(String dateofcollection) {
		this.dateofcollection = dateofcollection;
	}

	public List<Author> getAuthor() {
		return author;
	}

	public String getType() {
		return type;
	}

	public void setType(String type) {
		this.type = type;
	}

	public Container getContainer() {
		return container;
	}

	public void setContainer(Container container) {
		this.container = container;
	}

	public void setAuthor(List<Author> author) {
		this.author = author;
	}

	public Qualifier getLanguage() {
		return language;
	}

	public void setLanguage(Qualifier language) {
		this.language = language;
	}

	public List<Country> getCountry() {
		return country;
	}

	public void setCountry(List<Country> country) {
		this.country = country;
	}

	public List<Subject> getSubjects() {
		return subjects;
	}

	public void setSubjects(List<Subject> subjects) {
		this.subjects = subjects;
	}

	public String getMaintitle() {
		return maintitle;
	}

	public void setMaintitle(String maintitle) {
		this.maintitle = maintitle;
	}

	public String getSubtitle() {
		return subtitle;
	}

	public void setSubtitle(String subtitle) {
		this.subtitle = subtitle;
	}

	public List<String> getDescription() {
		return description;
	}

	public void setDescription(List<String> description) {
		this.description = description;
	}

	public String getPublicationdate() {
		return publicationdate;
	}

	public void setPublicationdate(String publicationdate) {
		this.publicationdate = publicationdate;
	}

	public String getPublisher() {
		return publisher;
	}

	public void setPublisher(String publisher) {
		this.publisher = publisher;
	}

	public String getEmbargoenddate() {
		return embargoenddate;
	}

	public void setEmbargoenddate(String embargoenddate) {
		this.embargoenddate = embargoenddate;
	}

	public List<String> getSource() {
		return source;
	}

	public void setSource(List<String> source) {
		this.source = source;
	}

	public List<String> getFormat() {
		return format;
	}

	public void setFormat(List<String> format) {
		this.format = format;
	}

	public List<String> getContributor() {
		return contributor;
	}

	public void setContributor(List<String> contributor) {
		this.contributor = contributor;
	}

	public List<String> getCoverage() {
		return coverage;
	}

	public void setCoverage(List<String> coverage) {
		this.coverage = coverage;
	}

	public AccessRight getBestaccessright() {
		return bestaccessright;
	}

	public void setBestaccessright(AccessRight bestaccessright) {
		this.bestaccessright = bestaccessright;
	}

	public List<Instance> getInstance() {
		return instance;
	}

	public void setInstance(List<Instance> instance) {
		this.instance = instance;
	}

	public List<String> getDocumentationUrl() {
		return documentationUrl;
	}

	public void setDocumentationUrl(List<String> documentationUrl) {
		this.documentationUrl = documentationUrl;
	}

	public String getCodeRepositoryUrl() {
		return codeRepositoryUrl;
	}

	public void setCodeRepositoryUrl(String codeRepositoryUrl) {
		this.codeRepositoryUrl = codeRepositoryUrl;
	}

	public String getProgrammingLanguage() {
		return programmingLanguage;
	}

	public void setProgrammingLanguage(String programmingLanguage) {
		this.programmingLanguage = programmingLanguage;
	}

	public List<String> getContactperson() {
		return contactperson;
	}

	public void setContactperson(List<String> contactperson) {
		this.contactperson = contactperson;
	}

	public List<String> getContactgroup() {
		return contactgroup;
	}

	public void setContactgroup(List<String> contactgroup) {
		this.contactgroup = contactgroup;
	}

	public List<String> getTool() {
		return tool;
	}

	public void setTool(List<String> tool) {
		this.tool = tool;
	}

	public String getSize() {
		return size;
	}

	public void setSize(String size) {
		this.size = size;
	}

	public String getVersion() {
		return version;
	}

	public void setVersion(String version) {
		this.version = version;
	}

	public List<GeoLocation> getGeolocation() {
		return geolocation;
	}

	public void setGeolocation(List<GeoLocation> geolocation) {
		this.geolocation = geolocation;
	}
}
