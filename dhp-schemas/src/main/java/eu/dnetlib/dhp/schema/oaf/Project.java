package eu.dnetlib.dhp.schema.oaf;

import java.io.Serializable;
import java.util.List;

public class Project extends OafEntity implements Serializable {

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

    public void setWebsiteurl(Field<String> websiteurl) {
        this.websiteurl = websiteurl;
    }

    public Field<String> getCode() {
        return code;
    }

    public void setCode(Field<String> code) {
        this.code = code;
    }

    public Field<String> getAcronym() {
        return acronym;
    }

    public void setAcronym(Field<String> acronym) {
        this.acronym = acronym;
    }

    public Field<String> getTitle() {
        return title;
    }

    public void setTitle(Field<String> title) {
        this.title = title;
    }

    public Field<String> getStartdate() {
        return startdate;
    }

    public void setStartdate(Field<String> startdate) {
        this.startdate = startdate;
    }

    public Field<String> getEnddate() {
        return enddate;
    }

    public void setEnddate(Field<String> enddate) {
        this.enddate = enddate;
    }

    public Field<String> getCallidentifier() {
        return callidentifier;
    }

    public void setCallidentifier(Field<String> callidentifier) {
        this.callidentifier = callidentifier;
    }

    public Field<String> getKeywords() {
        return keywords;
    }

    public void setKeywords(Field<String> keywords) {
        this.keywords = keywords;
    }

    public Field<String> getDuration() {
        return duration;
    }

    public void setDuration(Field<String> duration) {
        this.duration = duration;
    }

    public Field<String> getEcsc39() {
        return ecsc39;
    }

    public void setEcsc39(Field<String> ecsc39) {
        this.ecsc39 = ecsc39;
    }

    public Field<String> getOamandatepublications() {
        return oamandatepublications;
    }

    public void setOamandatepublications(Field<String> oamandatepublications) {
        this.oamandatepublications = oamandatepublications;
    }

    public Field<String> getEcarticle29_3() {
        return ecarticle29_3;
    }

    public void setEcarticle29_3(Field<String> ecarticle29_3) {
        this.ecarticle29_3 = ecarticle29_3;
    }

    public List<StructuredProperty> getSubjects() {
        return subjects;
    }

    public void setSubjects(List<StructuredProperty> subjects) {
        this.subjects = subjects;
    }

    public List<Field<String>> getFundingtree() {
        return fundingtree;
    }

    public void setFundingtree(List<Field<String>> fundingtree) {
        this.fundingtree = fundingtree;
    }

    public Qualifier getContracttype() {
        return contracttype;
    }

    public void setContracttype(Qualifier contracttype) {
        this.contracttype = contracttype;
    }

    public Field<String> getOptional1() {
        return optional1;
    }

    public void setOptional1(Field<String> optional1) {
        this.optional1 = optional1;
    }

    public Field<String> getOptional2() {
        return optional2;
    }

    public void setOptional2(Field<String> optional2) {
        this.optional2 = optional2;
    }

    public Field<String> getJsonextrainfo() {
        return jsonextrainfo;
    }

    public void setJsonextrainfo(Field<String> jsonextrainfo) {
        this.jsonextrainfo = jsonextrainfo;
    }

    public Field<String> getContactfullname() {
        return contactfullname;
    }

    public void setContactfullname(Field<String> contactfullname) {
        this.contactfullname = contactfullname;
    }

    public Field<String> getContactfax() {
        return contactfax;
    }

    public void setContactfax(Field<String> contactfax) {
        this.contactfax = contactfax;
    }

    public Field<String> getContactphone() {
        return contactphone;
    }

    public void setContactphone(Field<String> contactphone) {
        this.contactphone = contactphone;
    }

    public Field<String> getContactemail() {
        return contactemail;
    }

    public void setContactemail(Field<String> contactemail) {
        this.contactemail = contactemail;
    }

    public Field<String> getSummary() {
        return summary;
    }

    public void setSummary(Field<String> summary) {
        this.summary = summary;
    }

    public Field<String> getCurrency() {
        return currency;
    }

    public void setCurrency(Field<String> currency) {
        this.currency = currency;
    }

    public Float getTotalcost() {
        return totalcost;
    }

    public void setTotalcost(Float totalcost) {
        this.totalcost = totalcost;
    }

    public Float getFundedamount() {
        return fundedamount;
    }

    public void setFundedamount(Float fundedamount) {
        this.fundedamount = fundedamount;
    }
}
