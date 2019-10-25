package eu.dnetlib.dhp.schema.oaf;

import java.io.Serializable;
import java.util.List;

public abstract class Result extends OafEntity implements Serializable {

    private List<Author> author;

    // resulttype allows subclassing results into publications | datasets | software
    private Qualifier resulttype;

    // common fields
    private Qualifier language;
    
    private List<Qualifier> country;

    private List<StructuredProperty> subject;
    
    private List<StructuredProperty> title;
    
    private List<StructuredProperty> relevantdate;

    private List<Field<String>> description;
    
    private Field<String> dateofacceptance;
        
    private Field<String> publisher;
    
    private Field<String> embargoenddate;
    
    private List<Field<String>> source;
    
    private List<Field<String>> fulltext; // remove candidate
    
    private List<Field<String>> format;
    
    private List<Field<String>> contributor;
    
    private Qualifier resourcetype;
    
    private List<Field<String>> coverage;
    
    private Field<String> refereed; //peer-review status

    private List<Context> context;

    private List<ExternalReference> externalReference;

    private List<Instance> instance;

    public List<Author> getAuthor() {
        return author;
    }

    public Result setAuthor(List<Author> author) {
        this.author = author;
        return this;
    }

    public Qualifier getResulttype() {
        return resulttype;
    }

    public Result setResulttype(Qualifier resulttype) {
        this.resulttype = resulttype;
        return this;
    }

    public Qualifier getLanguage() {
        return language;
    }

    public Result setLanguage(Qualifier language) {
        this.language = language;
        return this;
    }

    public List<Qualifier> getCountry() {
        return country;
    }

    public Result setCountry(List<Qualifier> country) {
        this.country = country;
        return this;
    }

    public List<StructuredProperty> getSubject() {
        return subject;
    }

    public Result setSubject(List<StructuredProperty> subject) {
        this.subject = subject;
        return this;
    }

    public List<StructuredProperty> getTitle() {
        return title;
    }

    public Result setTitle(List<StructuredProperty> title) {
        this.title = title;
        return this;
    }

    public List<StructuredProperty> getRelevantdate() {
        return relevantdate;
    }

    public Result setRelevantdate(List<StructuredProperty> relevantdate) {
        this.relevantdate = relevantdate;
        return this;
    }

    public List<Field<String>> getDescription() {
        return description;
    }

    public Result setDescription(List<Field<String>> description) {
        this.description = description;
        return this;
    }

    public Field<String> getDateofacceptance() {
        return dateofacceptance;
    }

    public Result setDateofacceptance(Field<String> dateofacceptance) {
        this.dateofacceptance = dateofacceptance;
        return this;
    }

    public Field<String> getPublisher() {
        return publisher;
    }

    public Result setPublisher(Field<String> publisher) {
        this.publisher = publisher;
        return this;
    }

    public Field<String> getEmbargoenddate() {
        return embargoenddate;
    }

    public Result setEmbargoenddate(Field<String> embargoenddate) {
        this.embargoenddate = embargoenddate;
        return this;
    }

    public List<Field<String>> getSource() {
        return source;
    }

    public Result setSource(List<Field<String>> source) {
        this.source = source;
        return this;
    }

    public List<Field<String>> getFulltext() {
        return fulltext;
    }

    public Result setFulltext(List<Field<String>> fulltext) {
        this.fulltext = fulltext;
        return this;
    }

    public List<Field<String>> getFormat() {
        return format;
    }

    public Result setFormat(List<Field<String>> format) {
        this.format = format;
        return this;
    }

    public List<Field<String>> getContributor() {
        return contributor;
    }

    public Result setContributor(List<Field<String>> contributor) {
        this.contributor = contributor;
        return this;
    }

    public Qualifier getResourcetype() {
        return resourcetype;
    }

    public Result setResourcetype(Qualifier resourcetype) {
        this.resourcetype = resourcetype;
        return this;
    }

    public List<Field<String>> getCoverage() {
        return coverage;
    }

    public Result setCoverage(List<Field<String>> coverage) {
        this.coverage = coverage;
        return this;
    }

    public Field<String> getRefereed() {
        return refereed;
    }

    public Result setRefereed(Field<String> refereed) {
        this.refereed = refereed;
        return this;
    }

    public List<Context> getContext() {
        return context;
    }

    public Result setContext(List<Context> context) {
        this.context = context;
        return this;
    }

    public List<ExternalReference> getExternalReference() {
        return externalReference;
    }

    public Result setExternalReference(List<ExternalReference> externalReference) {
        this.externalReference = externalReference;
        return this;
    }

    public List<Instance> getInstance() {
        return instance;
    }

    public Result setInstance(List<Instance> instance) {
        this.instance = instance;
        return this;
    }
}
