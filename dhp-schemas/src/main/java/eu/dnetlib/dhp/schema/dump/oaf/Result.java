
package eu.dnetlib.dhp.schema.dump.oaf;

import java.io.Serializable;
import java.util.List;

import eu.dnetlib.dhp.schema.dump.oaf.community.Project;

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

	private List<ExternalReference> externalReference;

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

	public List<ExternalReference> getExternalReference() {
		return externalReference;
	}

	public void setExternalReference(List<ExternalReference> externalReference) {
		this.externalReference = externalReference;
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
