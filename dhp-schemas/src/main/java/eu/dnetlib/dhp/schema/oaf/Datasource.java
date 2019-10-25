package eu.dnetlib.dhp.schema.oaf;

import java.io.Serializable;
import java.util.List;

public class Datasource extends OafEntity implements Serializable {

    private Qualifier datasourcetype;

    private Qualifier openairecompatibility;

    private Field<String> officialname;

    private Field<String> englishname;

    private Field<String> websiteurl;

    private Field<String> logourl;

    private Field<String> contactemail;

    private Field<String> namespaceprefix;

    private Field<String> latitude;

    private Field<String> longitude;

    private Field<String> dateofvalidation;

    private Field<String> description;

    private List<StructuredProperty> subjects;

    // opendoar specific fields (od*)
    private Field<String> odnumberofitems;

    private Field<String> odnumberofitemsdate;

    private Field<String> odpolicies;

    private List<Field<String>> odlanguages;

    private List< Field<String>> odcontenttypes;

    private List< Field<String>> accessinfopackage;

    // re3data fields
    private Field<String> releasestartdate;

    private Field<String> releaseenddate;

    private Field<String> missionstatementurl;

    private Field<Boolean> dataprovider;

    private Field<Boolean> serviceprovider;

    // {open, restricted or closed}
    private Field<String> databaseaccesstype;

    // {open, restricted or closed}
    private Field<String> datauploadtype;

    // {feeRequired, registration, other}
    private Field<String> databaseaccessrestriction;

    // {feeRequired, registration, other}
    private Field<String> datauploadrestriction;

    private Field<Boolean> versioning;

    private Field<String> citationguidelineurl;

    //{yes, no, uknown}
    private Field<String> qualitymanagementkind;

    private Field<String> pidsystems;

    private Field<String> certificates;

    private List< KeyValue> policies;

    private Journal journal;

    public Qualifier getDatasourcetype() {
        return datasourcetype;
    }

    public void setDatasourcetype(Qualifier datasourcetype) {
        this.datasourcetype = datasourcetype;
    }

    public Qualifier getOpenairecompatibility() {
        return openairecompatibility;
    }

    public void setOpenairecompatibility(Qualifier openairecompatibility) {
        this.openairecompatibility = openairecompatibility;
    }

    public Field<String> getOfficialname() {
        return officialname;
    }

    public void setOfficialname(Field<String> officialname) {
        this.officialname = officialname;
    }

    public Field<String> getEnglishname() {
        return englishname;
    }

    public void setEnglishname(Field<String> englishname) {
        this.englishname = englishname;
    }

    public Field<String> getWebsiteurl() {
        return websiteurl;
    }

    public void setWebsiteurl(Field<String> websiteurl) {
        this.websiteurl = websiteurl;
    }

    public Field<String> getLogourl() {
        return logourl;
    }

    public void setLogourl(Field<String> logourl) {
        this.logourl = logourl;
    }

    public Field<String> getContactemail() {
        return contactemail;
    }

    public void setContactemail(Field<String> contactemail) {
        this.contactemail = contactemail;
    }

    public Field<String> getNamespaceprefix() {
        return namespaceprefix;
    }

    public void setNamespaceprefix(Field<String> namespaceprefix) {
        this.namespaceprefix = namespaceprefix;
    }

    public Field<String> getLatitude() {
        return latitude;
    }

    public void setLatitude(Field<String> latitude) {
        this.latitude = latitude;
    }

    public Field<String> getLongitude() {
        return longitude;
    }

    public void setLongitude(Field<String> longitude) {
        this.longitude = longitude;
    }

    public Field<String> getDateofvalidation() {
        return dateofvalidation;
    }

    public void setDateofvalidation(Field<String> dateofvalidation) {
        this.dateofvalidation = dateofvalidation;
    }

    public Field<String> getDescription() {
        return description;
    }

    public void setDescription(Field<String> description) {
        this.description = description;
    }

    public List<StructuredProperty> getSubjects() {
        return subjects;
    }

    public void setSubjects(List<StructuredProperty> subjects) {
        this.subjects = subjects;
    }

    public Field<String> getOdnumberofitems() {
        return odnumberofitems;
    }

    public void setOdnumberofitems(Field<String> odnumberofitems) {
        this.odnumberofitems = odnumberofitems;
    }

    public Field<String> getOdnumberofitemsdate() {
        return odnumberofitemsdate;
    }

    public void setOdnumberofitemsdate(Field<String> odnumberofitemsdate) {
        this.odnumberofitemsdate = odnumberofitemsdate;
    }

    public Field<String> getOdpolicies() {
        return odpolicies;
    }

    public void setOdpolicies(Field<String> odpolicies) {
        this.odpolicies = odpolicies;
    }

    public List<Field<String>> getOdlanguages() {
        return odlanguages;
    }

    public void setOdlanguages(List<Field<String>> odlanguages) {
        this.odlanguages = odlanguages;
    }

    public List<Field<String>> getOdcontenttypes() {
        return odcontenttypes;
    }

    public void setOdcontenttypes(List<Field<String>> odcontenttypes) {
        this.odcontenttypes = odcontenttypes;
    }

    public List<Field<String>> getAccessinfopackage() {
        return accessinfopackage;
    }

    public void setAccessinfopackage(List<Field<String>> accessinfopackage) {
        this.accessinfopackage = accessinfopackage;
    }

    public Field<String> getReleasestartdate() {
        return releasestartdate;
    }

    public void setReleasestartdate(Field<String> releasestartdate) {
        this.releasestartdate = releasestartdate;
    }

    public Field<String> getReleaseenddate() {
        return releaseenddate;
    }

    public void setReleaseenddate(Field<String> releaseenddate) {
        this.releaseenddate = releaseenddate;
    }

    public Field<String> getMissionstatementurl() {
        return missionstatementurl;
    }

    public void setMissionstatementurl(Field<String> missionstatementurl) {
        this.missionstatementurl = missionstatementurl;
    }

    public Field<Boolean> getDataprovider() {
        return dataprovider;
    }

    public void setDataprovider(Field<Boolean> dataprovider) {
        this.dataprovider = dataprovider;
    }

    public Field<Boolean> getServiceprovider() {
        return serviceprovider;
    }

    public void setServiceprovider(Field<Boolean> serviceprovider) {
        this.serviceprovider = serviceprovider;
    }

    public Field<String> getDatabaseaccesstype() {
        return databaseaccesstype;
    }

    public void setDatabaseaccesstype(Field<String> databaseaccesstype) {
        this.databaseaccesstype = databaseaccesstype;
    }

    public Field<String> getDatauploadtype() {
        return datauploadtype;
    }

    public void setDatauploadtype(Field<String> datauploadtype) {
        this.datauploadtype = datauploadtype;
    }

    public Field<String> getDatabaseaccessrestriction() {
        return databaseaccessrestriction;
    }

    public void setDatabaseaccessrestriction(Field<String> databaseaccessrestriction) {
        this.databaseaccessrestriction = databaseaccessrestriction;
    }

    public Field<String> getDatauploadrestriction() {
        return datauploadrestriction;
    }

    public void setDatauploadrestriction(Field<String> datauploadrestriction) {
        this.datauploadrestriction = datauploadrestriction;
    }

    public Field<Boolean> getVersioning() {
        return versioning;
    }

    public void setVersioning(Field<Boolean> versioning) {
        this.versioning = versioning;
    }

    public Field<String> getCitationguidelineurl() {
        return citationguidelineurl;
    }

    public void setCitationguidelineurl(Field<String> citationguidelineurl) {
        this.citationguidelineurl = citationguidelineurl;
    }

    public Field<String> getQualitymanagementkind() {
        return qualitymanagementkind;
    }

    public void setQualitymanagementkind(Field<String> qualitymanagementkind) {
        this.qualitymanagementkind = qualitymanagementkind;
    }

    public Field<String> getPidsystems() {
        return pidsystems;
    }

    public void setPidsystems(Field<String> pidsystems) {
        this.pidsystems = pidsystems;
    }

    public Field<String> getCertificates() {
        return certificates;
    }

    public void setCertificates(Field<String> certificates) {
        this.certificates = certificates;
    }

    public List<KeyValue> getPolicies() {
        return policies;
    }

    public void setPolicies(List<KeyValue> policies) {
        this.policies = policies;
    }

    public Journal getJournal() {
        return journal;
    }

    public void setJournal(Journal journal) {
        this.journal = journal;
    }
}
