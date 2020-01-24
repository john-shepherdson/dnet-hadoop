package eu.dnetlib.dhp.graph;

import eu.dnetlib.dhp.schema.oaf.KeyValue;
import eu.dnetlib.dhp.schema.oaf.Qualifier;
import eu.dnetlib.dhp.schema.oaf.StructuredProperty;

import java.io.Serializable;
import java.util.List;

public class RelatedEntity implements Serializable {

    // results
    private StructuredProperty title; // also for projects
    private String dateofacceptance;
    private String publisher;
    private List<StructuredProperty> pid;
    private String codeRepositoryUrl;
    private Qualifier resulttype;
    private List<KeyValue> collectedfrom;

    // datasource
    private String officialname;
    private String websiteurl; // also for organizations, projects
    private Qualifier datasourcetype;
    private Qualifier datasourcetypeui;
    //private String aggregatortype;
    private Qualifier openairecompatibility;

    // organization
    private String legalname;
    private String legalshortname;
    private Qualifier country;

    // project
    private String code;
    private String acronym;
    private Qualifier contracttype;
    private String fundingtree;

    public StructuredProperty getTitle() {
        return title;
    }

    public RelatedEntity setTitle(StructuredProperty title) {
        this.title = title;
        return this;
    }

    public String getDateofacceptance() {
        return dateofacceptance;
    }

    public RelatedEntity setDateofacceptance(String dateofacceptance) {
        this.dateofacceptance = dateofacceptance;
        return this;
    }

    public String getPublisher() {
        return publisher;
    }

    public RelatedEntity setPublisher(String publisher) {
        this.publisher = publisher;
        return this;
    }

    public List<StructuredProperty> getPid() {
        return pid;
    }

    public RelatedEntity setPid(List<StructuredProperty> pid) {
        this.pid = pid;
        return this;
    }

    public String getCodeRepositoryUrl() {
        return codeRepositoryUrl;
    }

    public RelatedEntity setCodeRepositoryUrl(String codeRepositoryUrl) {
        this.codeRepositoryUrl = codeRepositoryUrl;
        return this;
    }

    public Qualifier getResulttype() {
        return resulttype;
    }

    public RelatedEntity setResulttype(Qualifier resulttype) {
        this.resulttype = resulttype;
        return this;
    }

    public List<KeyValue> getCollectedfrom() {
        return collectedfrom;
    }

    public RelatedEntity setCollectedfrom(List<KeyValue> collectedfrom) {
        this.collectedfrom = collectedfrom;
        return this;
    }

    public String getOfficialname() {
        return officialname;
    }

    public RelatedEntity setOfficialname(String officialname) {
        this.officialname = officialname;
        return this;
    }

    public String getWebsiteurl() {
        return websiteurl;
    }

    public RelatedEntity setWebsiteurl(String websiteurl) {
        this.websiteurl = websiteurl;
        return this;
    }

    public Qualifier getDatasourcetype() {
        return datasourcetype;
    }

    public RelatedEntity setDatasourcetype(Qualifier datasourcetype) {
        this.datasourcetype = datasourcetype;
        return this;
    }

    public Qualifier getDatasourcetypeui() {
        return datasourcetypeui;
    }

    public RelatedEntity setDatasourcetypeui(Qualifier datasourcetypeui) {
        this.datasourcetypeui = datasourcetypeui;
        return this;
    }

    public Qualifier getOpenairecompatibility() {
        return openairecompatibility;
    }

    public RelatedEntity setOpenairecompatibility(Qualifier openairecompatibility) {
        this.openairecompatibility = openairecompatibility;
        return this;
    }

    public String getLegalname() {
        return legalname;
    }

    public RelatedEntity setLegalname(String legalname) {
        this.legalname = legalname;
        return this;
    }

    public String getLegalshortname() {
        return legalshortname;
    }

    public RelatedEntity setLegalshortname(String legalshortname) {
        this.legalshortname = legalshortname;
        return this;
    }

    public Qualifier getCountry() {
        return country;
    }

    public RelatedEntity setCountry(Qualifier country) {
        this.country = country;
        return this;
    }

    public String getCode() {
        return code;
    }

    public RelatedEntity setCode(String code) {
        this.code = code;
        return this;
    }

    public String getAcronym() {
        return acronym;
    }

    public RelatedEntity setAcronym(String acronym) {
        this.acronym = acronym;
        return this;
    }

    public Qualifier getContracttype() {
        return contracttype;
    }

    public RelatedEntity setContracttype(Qualifier contracttype) {
        this.contracttype = contracttype;
        return this;
    }

    public String getFundingtree() {
        return fundingtree;
    }

    public RelatedEntity setFundingtree(String fundingtree) {
        this.fundingtree = fundingtree;
        return this;
    }
}
