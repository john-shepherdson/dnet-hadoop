
package eu.dnetlib.dhp.oa.provision.model;

import java.io.Serializable;
import java.util.List;

import com.google.common.base.Objects;

import eu.dnetlib.dhp.schema.oaf.Instance;
import eu.dnetlib.dhp.schema.oaf.KeyValue;
import eu.dnetlib.dhp.schema.oaf.Qualifier;
import eu.dnetlib.dhp.schema.oaf.StructuredProperty;

public class RelatedEntity implements Serializable {

	private static final long serialVersionUID = -4982643490443810597L;

	private String id;
	private String type;

	// common fields
	private StructuredProperty title;
	private String websiteurl; // datasource, organizations, projects

	// results
	private String dateofacceptance;
	private String publisher;
	private List<StructuredProperty> pid;
	private String codeRepositoryUrl;
	private Qualifier resulttype;
	private List<KeyValue> collectedfrom;
	private List<Instance> instances;

	// datasource
	private String officialname;
	private Qualifier datasourcetype;
	private Qualifier datasourcetypeui;
	private Qualifier openairecompatibility;

	// organization
	private String legalname;
	private String legalshortname;
	private Qualifier country;

	// project
	private String projectTitle;
	private String code;
	private String acronym;
	private Qualifier contracttype;
	private List<String> fundingtree;

	public String getId() {
		return id;
	}

	public void setId(String id) {
		this.id = id;
	}

	public String getType() {
		return type;
	}

	public void setType(String type) {
		this.type = type;
	}

	public StructuredProperty getTitle() {
		return title;
	}

	public void setTitle(StructuredProperty title) {
		this.title = title;
	}

	public String getWebsiteurl() {
		return websiteurl;
	}

	public void setWebsiteurl(String websiteurl) {
		this.websiteurl = websiteurl;
	}

	public String getDateofacceptance() {
		return dateofacceptance;
	}

	public void setDateofacceptance(String dateofacceptance) {
		this.dateofacceptance = dateofacceptance;
	}

	public String getPublisher() {
		return publisher;
	}

	public void setPublisher(String publisher) {
		this.publisher = publisher;
	}

	public List<StructuredProperty> getPid() {
		return pid;
	}

	public void setPid(List<StructuredProperty> pid) {
		this.pid = pid;
	}

	public String getCodeRepositoryUrl() {
		return codeRepositoryUrl;
	}

	public void setCodeRepositoryUrl(String codeRepositoryUrl) {
		this.codeRepositoryUrl = codeRepositoryUrl;
	}

	public Qualifier getResulttype() {
		return resulttype;
	}

	public void setResulttype(Qualifier resulttype) {
		this.resulttype = resulttype;
	}

	public List<KeyValue> getCollectedfrom() {
		return collectedfrom;
	}

	public void setCollectedfrom(List<KeyValue> collectedfrom) {
		this.collectedfrom = collectedfrom;
	}

	public List<Instance> getInstances() {
		return instances;
	}

	public void setInstances(List<Instance> instances) {
		this.instances = instances;
	}

	public String getOfficialname() {
		return officialname;
	}

	public void setOfficialname(String officialname) {
		this.officialname = officialname;
	}

	public Qualifier getDatasourcetype() {
		return datasourcetype;
	}

	public void setDatasourcetype(Qualifier datasourcetype) {
		this.datasourcetype = datasourcetype;
	}

	public Qualifier getDatasourcetypeui() {
		return datasourcetypeui;
	}

	public void setDatasourcetypeui(Qualifier datasourcetypeui) {
		this.datasourcetypeui = datasourcetypeui;
	}

	public Qualifier getOpenairecompatibility() {
		return openairecompatibility;
	}

	public void setOpenairecompatibility(Qualifier openairecompatibility) {
		this.openairecompatibility = openairecompatibility;
	}

	public String getLegalname() {
		return legalname;
	}

	public void setLegalname(String legalname) {
		this.legalname = legalname;
	}

	public String getLegalshortname() {
		return legalshortname;
	}

	public void setLegalshortname(String legalshortname) {
		this.legalshortname = legalshortname;
	}

	public Qualifier getCountry() {
		return country;
	}

	public void setCountry(Qualifier country) {
		this.country = country;
	}

	public String getProjectTitle() {
		return projectTitle;
	}

	public void setProjectTitle(String projectTitle) {
		this.projectTitle = projectTitle;
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

	public Qualifier getContracttype() {
		return contracttype;
	}

	public void setContracttype(Qualifier contracttype) {
		this.contracttype = contracttype;
	}

	public List<String> getFundingtree() {
		return fundingtree;
	}

	public void setFundingtree(List<String> fundingtree) {
		this.fundingtree = fundingtree;
	}

	@Override
	public boolean equals(Object o) {
		if (this == o)
			return true;
		if (o == null || getClass() != o.getClass())
			return false;
		RelatedEntity that = (RelatedEntity) o;
		return Objects.equal(id, that.id)
			&& Objects.equal(type, that.type)
			&& Objects.equal(title, that.title)
			&& Objects.equal(websiteurl, that.websiteurl)
			&& Objects.equal(dateofacceptance, that.dateofacceptance)
			&& Objects.equal(publisher, that.publisher)
			&& Objects.equal(pid, that.pid)
			&& Objects.equal(codeRepositoryUrl, that.codeRepositoryUrl)
			&& Objects.equal(resulttype, that.resulttype)
			&& Objects.equal(collectedfrom, that.collectedfrom)
			&& Objects.equal(instances, that.instances)
			&& Objects.equal(officialname, that.officialname)
			&& Objects.equal(datasourcetype, that.datasourcetype)
			&& Objects.equal(datasourcetypeui, that.datasourcetypeui)
			&& Objects.equal(openairecompatibility, that.openairecompatibility)
			&& Objects.equal(legalname, that.legalname)
			&& Objects.equal(legalshortname, that.legalshortname)
			&& Objects.equal(country, that.country)
			&& Objects.equal(projectTitle, that.projectTitle)
			&& Objects.equal(code, that.code)
			&& Objects.equal(acronym, that.acronym)
			&& Objects.equal(contracttype, that.contracttype)
			&& Objects.equal(fundingtree, that.fundingtree);
	}

	@Override
	public int hashCode() {
		return Objects
			.hashCode(
				id,
				type,
				title,
				websiteurl,
				dateofacceptance,
				publisher,
				pid,
				codeRepositoryUrl,
				resulttype,
				collectedfrom,
				instances,
				officialname,
				datasourcetype,
				datasourcetypeui,
				openairecompatibility,
				legalname,
				legalshortname,
				country,
				projectTitle,
				code,
				acronym,
				contracttype,
				fundingtree);
	}
}
