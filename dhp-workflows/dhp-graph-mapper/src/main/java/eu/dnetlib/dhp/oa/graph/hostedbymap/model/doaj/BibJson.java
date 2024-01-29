
package eu.dnetlib.dhp.oa.graph.hostedbymap.model.doaj;

import java.io.Serializable;
import java.util.List;

public class BibJson implements Serializable {
	private Editorial editorial;
	private PidScheme pid_scheme;
	private Copyright copyright;
	private List<String> keywords;
	private Plagiarism plagiarism;
	private List<Subject> subject;
	private String eissn;
	private String pissn;
	private List<String> language;
	private String title;
	private Article article;
	private Institution institution;
	private Preservation preservation;
	private List<License> license;
	private Ref ref;
	private Integer oa_start;
	private APC apc;
	private OtherCharges other_charges;
	private Integer publication_time_weeks;
	private DepositPolicy deposit_policy;
	private Publisher publisher;
	private Boolean boai;
	private Waiver waiver;
	private String alternative_title;
	private List<String> is_replaced_by;
	private List<String> replaces;
	private String discontinued_date;

	public String getDiscontinued_date() {
		return discontinued_date;
	}

	public void setDiscontinued_date(String discontinued_date) {
		this.discontinued_date = discontinued_date;
	}

	public List<String> getReplaces() {
		return replaces;
	}

	public void setReplaces(List<String> replaces) {
		this.replaces = replaces;
	}

	public List<String> getIs_replaced_by() {
		return is_replaced_by;
	}

	public void setIs_replaced_by(List<String> is_replaced_by) {
		this.is_replaced_by = is_replaced_by;
	}

	public String getAlternative_title() {
		return alternative_title;
	}

	public void setAlternative_title(String alternative_title) {
		this.alternative_title = alternative_title;
	}

	public String getPissn() {
		return pissn;
	}

	public void setPissn(String pissn) {
		this.pissn = pissn;
	}

	public Editorial getEditorial() {
		return editorial;
	}

	public void setEditorial(Editorial editorial) {
		this.editorial = editorial;
	}

	public PidScheme getPid_scheme() {
		return pid_scheme;
	}

	public void setPid_scheme(PidScheme pid_scheme) {
		this.pid_scheme = pid_scheme;
	}

	public Copyright getCopyright() {
		return copyright;
	}

	public void setCopyright(Copyright copyright) {
		this.copyright = copyright;
	}

	public List<String> getKeywords() {
		return keywords;
	}

	public void setKeywords(List<String> keywords) {
		this.keywords = keywords;
	}

	public Plagiarism getPlagiarism() {
		return plagiarism;
	}

	public void setPlagiarism(Plagiarism plagiarism) {
		this.plagiarism = plagiarism;
	}

	public List<Subject> getSubject() {
		return subject;
	}

	public void setSubject(List<Subject> subject) {
		this.subject = subject;
	}

	public String getEissn() {
		return eissn;
	}

	public void setEissn(String eissn) {
		this.eissn = eissn;
	}

	public List<String> getLanguage() {
		return language;
	}

	public void setLanguage(List<String> language) {
		this.language = language;
	}

	public String getTitle() {
		return title;
	}

	public void setTitle(String title) {
		this.title = title;
	}

	public Article getArticle() {
		return article;
	}

	public void setArticle(Article article) {
		this.article = article;
	}

	public Institution getInstitution() {
		return institution;
	}

	public void setInstitution(Institution institution) {
		this.institution = institution;
	}

	public Preservation getPreservation() {
		return preservation;
	}

	public void setPreservation(Preservation preservation) {
		this.preservation = preservation;
	}

	public List<License> getLicense() {
		return license;
	}

	public void setLicense(List<License> license) {
		this.license = license;
	}

	public Ref getRef() {
		return ref;
	}

	public void setRef(Ref ref) {
		this.ref = ref;
	}

	public Integer getOa_start() {
		return oa_start;
	}

	public void setOa_start(Integer oa_start) {
		this.oa_start = oa_start;
	}

	public APC getApc() {
		return apc;
	}

	public void setApc(APC apc) {
		this.apc = apc;
	}

	public OtherCharges getOther_charges() {
		return other_charges;
	}

	public void setOther_charges(OtherCharges other_charges) {
		this.other_charges = other_charges;
	}

	public Integer getPublication_time_weeks() {
		return publication_time_weeks;
	}

	public void setPublication_time_weeks(Integer publication_time_weeks) {
		this.publication_time_weeks = publication_time_weeks;
	}

	public DepositPolicy getDeposit_policy() {
		return deposit_policy;
	}

	public void setDeposit_policy(DepositPolicy deposit_policy) {
		this.deposit_policy = deposit_policy;
	}

	public Publisher getPublisher() {
		return publisher;
	}

	public void setPublisher(Publisher publisher) {
		this.publisher = publisher;
	}

	public Boolean getBoai() {
		return boai;
	}

	public void setBoai(Boolean boai) {
		this.boai = boai;
	}

	public Waiver getWaiver() {
		return waiver;
	}

	public void setWaiver(Waiver waiver) {
		this.waiver = waiver;
	}
}
