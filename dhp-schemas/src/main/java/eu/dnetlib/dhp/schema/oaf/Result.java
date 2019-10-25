package eu.dnetlib.dhp.schema.oaf;

import java.io.Serializable;
import java.util.List;

public abstract class Result<T extends Result<T>> extends OafEntity<T> implements Serializable {

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

    public T setAuthor(List<Author> author) {
        this.author = author;
        return self();
    }

    public Qualifier getResulttype() {
        return resulttype;
    }

    public T setResulttype(Qualifier resulttype) {
        this.resulttype = resulttype;
        return self();
    }

    public Qualifier getLanguage() {
        return language;
    }

    public T setLanguage(Qualifier language) {
        this.language = language;
        return self();
    }

    public List<Qualifier> getCountry() {
        return country;
    }

    public T setCountry(List<Qualifier> country) {
        this.country = country;
        return self();
    }

    public List<StructuredProperty> getSubject() {
        return subject;
    }

    public T setSubject(List<StructuredProperty> subject) {
        this.subject = subject;
        return self();
    }

    public List<StructuredProperty> getTitle() {
        return title;
    }

    public T setTitle(List<StructuredProperty> title) {
        this.title = title;
        return self();
    }

    public List<StructuredProperty> getRelevantdate() {
        return relevantdate;
    }

    public T setRelevantdate(List<StructuredProperty> relevantdate) {
        this.relevantdate = relevantdate;
        return self();
    }

    public List<Field<String>> getDescription() {
        return description;
    }

    public T setDescription(List<Field<String>> description) {
        this.description = description;
        return self();
    }

    public Field<String> getDateofacceptance() {
        return dateofacceptance;
    }

    public T setDateofacceptance(Field<String> dateofacceptance) {
        this.dateofacceptance = dateofacceptance;
        return self();
    }

    public Field<String> getPublisher() {
        return publisher;
    }

    public T setPublisher(Field<String> publisher) {
        this.publisher = publisher;
        return self();
    }

    public Field<String> getEmbargoenddate() {
        return embargoenddate;
    }

    public T setEmbargoenddate(Field<String> embargoenddate) {
        this.embargoenddate = embargoenddate;
        return self();
    }

    public List<Field<String>> getSource() {
        return source;
    }

    public T setSource(List<Field<String>> source) {
        this.source = source;
        return self();
    }

    public List<Field<String>> getFulltext() {
        return fulltext;
    }

    public T setFulltext(List<Field<String>> fulltext) {
        this.fulltext = fulltext;
        return self();
    }

    public List<Field<String>> getFormat() {
        return format;
    }

    public T setFormat(List<Field<String>> format) {
        this.format = format;
        return self();
    }

    public List<Field<String>> getContributor() {
        return contributor;
    }

    public T setContributor(List<Field<String>> contributor) {
        this.contributor = contributor;
        return self();
    }

    public Qualifier getResourcetype() {
        return resourcetype;
    }

    public T setResourcetype(Qualifier resourcetype) {
        this.resourcetype = resourcetype;
        return self();
    }

    public List<Field<String>> getCoverage() {
        return coverage;
    }

    public T setCoverage(List<Field<String>> coverage) {
        this.coverage = coverage;
        return self();
    }

    public Field<String> getRefereed() {
        return refereed;
    }

    public T setRefereed(Field<String> refereed) {
        this.refereed = refereed;
        return self();
    }

    public List<Context> getContext() {
        return context;
    }

    public T setContext(List<Context> context) {
        this.context = context;
        return self();
    }

    public List<ExternalReference> getExternalReference() {
        return externalReference;
    }

    public T setExternalReference(List<ExternalReference> externalReference) {
        this.externalReference = externalReference;
        return self();
    }

    public List<Instance> getInstance() {
        return instance;
    }

    public T setInstance(List<Instance> instance) {
        this.instance = instance;
        return self();
    }
}
