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

    public Datasource setDatasourcetype(Qualifier datasourcetype) {
        this.datasourcetype = datasourcetype;
        return this;
    }

    public Qualifier getOpenairecompatibility() {
        return openairecompatibility;
    }

    public Datasource setOpenairecompatibility(Qualifier openairecompatibility) {
        this.openairecompatibility = openairecompatibility;
        return this;
    }

    public Field<String> getOfficialname() {
        return officialname;
    }

    public Datasource setOfficialname(Field<String> officialname) {
        this.officialname = officialname;
        return this;
    }

    public Field<String> getEnglishname() {
        return englishname;
    }

    public Datasource setEnglishname(Field<String> englishname) {
        this.englishname = englishname;
        return this;
    }

    public Field<String> getWebsiteurl() {
        return websiteurl;
    }

    public Datasource setWebsiteurl(Field<String> websiteurl) {
        this.websiteurl = websiteurl;
        return this;
    }

    public Field<String> getLogourl() {
        return logourl;
    }

    public Datasource setLogourl(Field<String> logourl) {
        this.logourl = logourl;
        return this;
    }

    public Field<String> getContactemail() {
        return contactemail;
    }

    public Datasource setContactemail(Field<String> contactemail) {
        this.contactemail = contactemail;
        return this;
    }

    public Field<String> getNamespaceprefix() {
        return namespaceprefix;
    }

    public Datasource setNamespaceprefix(Field<String> namespaceprefix) {
        this.namespaceprefix = namespaceprefix;
        return this;
    }

    public Field<String> getLatitude() {
        return latitude;
    }

    public Datasource setLatitude(Field<String> latitude) {
        this.latitude = latitude;
        return this;
    }

    public Field<String> getLongitude() {
        return longitude;
    }

    public Datasource setLongitude(Field<String> longitude) {
        this.longitude = longitude;
        return this;
    }

    public Field<String> getDateofvalidation() {
        return dateofvalidation;
    }

    public Datasource setDateofvalidation(Field<String> dateofvalidation) {
        this.dateofvalidation = dateofvalidation;
        return this;
    }

    public Field<String> getDescription() {
        return description;
    }

    public Datasource setDescription(Field<String> description) {
        this.description = description;
        return this;
    }

    public List<StructuredProperty> getSubjects() {
        return subjects;
    }

    public Datasource setSubjects(List<StructuredProperty> subjects) {
        this.subjects = subjects;
        return this;
    }

    public Field<String> getOdnumberofitems() {
        return odnumberofitems;
    }

    public Datasource setOdnumberofitems(Field<String> odnumberofitems) {
        this.odnumberofitems = odnumberofitems;
        return this;
    }

    public Field<String> getOdnumberofitemsdate() {
        return odnumberofitemsdate;
    }

    public Datasource setOdnumberofitemsdate(Field<String> odnumberofitemsdate) {
        this.odnumberofitemsdate = odnumberofitemsdate;
        return this;
    }

    public Field<String> getOdpolicies() {
        return odpolicies;
    }

    public Datasource setOdpolicies(Field<String> odpolicies) {
        this.odpolicies = odpolicies;
        return this;
    }

    public List<Field<String>> getOdlanguages() {
        return odlanguages;
    }

    public Datasource setOdlanguages(List<Field<String>> odlanguages) {
        this.odlanguages = odlanguages;
        return this;
    }

    public List<Field<String>> getOdcontenttypes() {
        return odcontenttypes;
    }

    public Datasource setOdcontenttypes(List<Field<String>> odcontenttypes) {
        this.odcontenttypes = odcontenttypes;
        return this;
    }

    public List<Field<String>> getAccessinfopackage() {
        return accessinfopackage;
    }

    public Datasource setAccessinfopackage(List<Field<String>> accessinfopackage) {
        this.accessinfopackage = accessinfopackage;
        return this;
    }

    public Field<String> getReleasestartdate() {
        return releasestartdate;
    }

    public Datasource setReleasestartdate(Field<String> releasestartdate) {
        this.releasestartdate = releasestartdate;
        return this;
    }

    public Field<String> getReleaseenddate() {
        return releaseenddate;
    }

    public Datasource setReleaseenddate(Field<String> releaseenddate) {
        this.releaseenddate = releaseenddate;
        return this;
    }

    public Field<String> getMissionstatementurl() {
        return missionstatementurl;
    }

    public Datasource setMissionstatementurl(Field<String> missionstatementurl) {
        this.missionstatementurl = missionstatementurl;
        return this;
    }

    public Field<Boolean> getDataprovider() {
        return dataprovider;
    }

    public Datasource setDataprovider(Field<Boolean> dataprovider) {
        this.dataprovider = dataprovider;
        return this;
    }

    public Field<Boolean> getServiceprovider() {
        return serviceprovider;
    }

    public Datasource setServiceprovider(Field<Boolean> serviceprovider) {
        this.serviceprovider = serviceprovider;
        return this;
    }

    public Field<String> getDatabaseaccesstype() {
        return databaseaccesstype;
    }

    public Datasource setDatabaseaccesstype(Field<String> databaseaccesstype) {
        this.databaseaccesstype = databaseaccesstype;
        return this;
    }

    public Field<String> getDatauploadtype() {
        return datauploadtype;
    }

    public Datasource setDatauploadtype(Field<String> datauploadtype) {
        this.datauploadtype = datauploadtype;
        return this;
    }

    public Field<String> getDatabaseaccessrestriction() {
        return databaseaccessrestriction;
    }

    public Datasource setDatabaseaccessrestriction(Field<String> databaseaccessrestriction) {
        this.databaseaccessrestriction = databaseaccessrestriction;
        return this;
    }

    public Field<String> getDatauploadrestriction() {
        return datauploadrestriction;
    }

    public Datasource setDatauploadrestriction(Field<String> datauploadrestriction) {
        this.datauploadrestriction = datauploadrestriction;
        return this;
    }

    public Field<Boolean> getVersioning() {
        return versioning;
    }

    public Datasource setVersioning(Field<Boolean> versioning) {
        this.versioning = versioning;
        return this;
    }

    public Field<String> getCitationguidelineurl() {
        return citationguidelineurl;
    }

    public Datasource setCitationguidelineurl(Field<String> citationguidelineurl) {
        this.citationguidelineurl = citationguidelineurl;
        return this;
    }

    public Field<String> getQualitymanagementkind() {
        return qualitymanagementkind;
    }

    public Datasource setQualitymanagementkind(Field<String> qualitymanagementkind) {
        this.qualitymanagementkind = qualitymanagementkind;
        return this;
    }

    public Field<String> getPidsystems() {
        return pidsystems;
    }

    public Datasource setPidsystems(Field<String> pidsystems) {
        this.pidsystems = pidsystems;
        return this;
    }

    public Field<String> getCertificates() {
        return certificates;
    }

    public Datasource setCertificates(Field<String> certificates) {
        this.certificates = certificates;
        return this;
    }

    public List<KeyValue> getPolicies() {
        return policies;
    }

    public Datasource setPolicies(List<KeyValue> policies) {
        this.policies = policies;
        return this;
    }

    public Journal getJournal() {
        return journal;
    }

    public Datasource setJournal(Journal journal) {
        this.journal = journal;
        return this;
    }
}
