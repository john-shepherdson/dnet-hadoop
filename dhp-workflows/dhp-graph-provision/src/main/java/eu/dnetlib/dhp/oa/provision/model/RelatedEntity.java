package eu.dnetlib.dhp.oa.provision.model;

import eu.dnetlib.dhp.schema.oaf.Instance;
import eu.dnetlib.dhp.schema.oaf.KeyValue;
import eu.dnetlib.dhp.schema.oaf.Qualifier;
import eu.dnetlib.dhp.schema.oaf.StructuredProperty;

import java.io.Serializable;
import java.util.List;

public class RelatedEntity implements Serializable {

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
    //private String aggregatortype;

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
}