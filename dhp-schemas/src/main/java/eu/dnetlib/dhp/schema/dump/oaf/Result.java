
package eu.dnetlib.dhp.schema.dump.oaf;


import java.io.Serializable;
import java.util.List;


public class Result extends OafEntity implements Serializable {

	private List<Author> author;

	// resulttype allows subclassing results into publications | datasets | software
	private String type; //resulttype

	// common fields
	private Qualifier language;

	private List<Country> country;

	private List<ControlledField> subject;

	private String maintitle;

	private String subtitle;


	private List<String> description;

	private String publicationdata; // dateofacceptance;

	private String publisher;

	private String embargoenddate;

	private List<String> source;

	private List<String> format;

	private List<String> contributor;

	private List<String> coverage;

	private AccessRight bestaccessright;

	private List<Context> context;

	private List<ExternalReference> externalReference;

	private List<Instance> instance;

	public List<Author> getAuthor() {
		return author;
	}

	public void setAuthor(List<Author> author) {
		this.author = author;
	}

	public String getResulttype() {
		return type;
	}

	public void setResulttype(String resulttype) {
		this.type = resulttype;
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

	public List<ControlledField> getSubject() {
		return subject;
	}

	public void setSubject(List<ControlledField> subject) {
		this.subject = subject;
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

	public String getPublicationdata() {
		return publicationdata;
	}

	public void setPublicationdata(String publicationdata) {
		this.publicationdata = publicationdata;
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

	public List<Context> getContext() {
		return context;
	}

	public void setContext(List<Context> context) {
		this.context = context;
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
}
