package eu.dnetlib.dhp.schema.oaf;

import java.io.Serializable;
import java.util.List;

public class Project extends OafEntity<Project> implements Serializable {

    private Field<String> websiteurl;

    private Field<String> code;

    private Field<String> acronym;

    private Field<String> title;

    private Field<String> startdate;

    private Field<String> enddate;

    private Field<String> callidentifier;

    private Field<String> keywords;

    private Field<String> duration;

    private Field<String> ecsc39;

    private Field<String> oamandatepublications;

    private Field<String> ecarticle29_3;

    private List<StructuredProperty> subjects;

    private List<Field<String>> fundingtree;

    private Qualifier contracttype;

    private Field<String> optional1;

    private Field<String> optional2;

    private Field<String> jsonextrainfo;

    private Field<String> contactfullname;

    private Field<String> contactfax;

    private Field<String> contactphone;

    private Field<String> contactemail;

    private Field<String> summary;

    private Field<String> currency;

    private Float totalcost;

    private Float fundedamount;

    public Field<String> getWebsiteurl() {
        return websiteurl;
    }

    public Project setWebsiteurl(Field<String> websiteurl) {
        this.websiteurl = websiteurl;
        return this;
    }

    public Field<String> getCode() {
        return code;
    }

    public Project setCode(Field<String> code) {
        this.code = code;
        return this;
    }

    public Field<String> getAcronym() {
        return acronym;
    }

    public Project setAcronym(Field<String> acronym) {
        this.acronym = acronym;
        return this;
    }

    public Field<String> getTitle() {
        return title;
    }

    public Project setTitle(Field<String> title) {
        this.title = title;
        return this;
    }

    public Field<String> getStartdate() {
        return startdate;
    }

    public Project setStartdate(Field<String> startdate) {
        this.startdate = startdate;
        return this;
    }

    public Field<String> getEnddate() {
        return enddate;
    }

    public Project setEnddate(Field<String> enddate) {
        this.enddate = enddate;
        return this;
    }

    public Field<String> getCallidentifier() {
        return callidentifier;
    }

    public Project setCallidentifier(Field<String> callidentifier) {
        this.callidentifier = callidentifier;
        return this;
    }

    public Field<String> getKeywords() {
        return keywords;
    }

    public Project setKeywords(Field<String> keywords) {
        this.keywords = keywords;
        return this;
    }

    public Field<String> getDuration() {
        return duration;
    }

    public Project setDuration(Field<String> duration) {
        this.duration = duration;
        return this;
    }

    public Field<String> getEcsc39() {
        return ecsc39;
    }

    public Project setEcsc39(Field<String> ecsc39) {
        this.ecsc39 = ecsc39;
        return this;
    }

    public Field<String> getOamandatepublications() {
        return oamandatepublications;
    }

    public Project setOamandatepublications(Field<String> oamandatepublications) {
        this.oamandatepublications = oamandatepublications;
        return this;
    }

    public Field<String> getEcarticle29_3() {
        return ecarticle29_3;
    }

    public Project setEcarticle29_3(Field<String> ecarticle29_3) {
        this.ecarticle29_3 = ecarticle29_3;
        return this;
    }

    public List<StructuredProperty> getSubjects() {
        return subjects;
    }

    public Project setSubjects(List<StructuredProperty> subjects) {
        this.subjects = subjects;
        return this;
    }

    public List<Field<String>> getFundingtree() {
        return fundingtree;
    }

    public Project setFundingtree(List<Field<String>> fundingtree) {
        this.fundingtree = fundingtree;
        return this;
    }

    public Qualifier getContracttype() {
        return contracttype;
    }

    public Project setContracttype(Qualifier contracttype) {
        this.contracttype = contracttype;
        return this;
    }

    public Field<String> getOptional1() {
        return optional1;
    }

    public Project setOptional1(Field<String> optional1) {
        this.optional1 = optional1;
        return this;
    }

    public Field<String> getOptional2() {
        return optional2;
    }

    public Project setOptional2(Field<String> optional2) {
        this.optional2 = optional2;
        return this;
    }

    public Field<String> getJsonextrainfo() {
        return jsonextrainfo;
    }

    public Project setJsonextrainfo(Field<String> jsonextrainfo) {
        this.jsonextrainfo = jsonextrainfo;
        return this;
    }

    public Field<String> getContactfullname() {
        return contactfullname;
    }

    public Project setContactfullname(Field<String> contactfullname) {
        this.contactfullname = contactfullname;
        return this;
    }

    public Field<String> getContactfax() {
        return contactfax;
    }

    public Project setContactfax(Field<String> contactfax) {
        this.contactfax = contactfax;
        return this;
    }

    public Field<String> getContactphone() {
        return contactphone;
    }

    public Project setContactphone(Field<String> contactphone) {
        this.contactphone = contactphone;
        return this;
    }

    public Field<String> getContactemail() {
        return contactemail;
    }

    public Project setContactemail(Field<String> contactemail) {
        this.contactemail = contactemail;
        return this;
    }

    public Field<String> getSummary() {
        return summary;
    }

    public Project setSummary(Field<String> summary) {
        this.summary = summary;
        return this;
    }

    public Field<String> getCurrency() {
        return currency;
    }

    public Project setCurrency(Field<String> currency) {
        this.currency = currency;
        return this;
    }

    public Float getTotalcost() {
        return totalcost;
    }

    public Project setTotalcost(Float totalcost) {
        this.totalcost = totalcost;
        return this;
    }

    public Float getFundedamount() {
        return fundedamount;
    }

    public Project setFundedamount(Float fundedamount) {
        this.fundedamount = fundedamount;
        return this;
    }

    @Override
    protected Project self() {
        return this;
    }
}
