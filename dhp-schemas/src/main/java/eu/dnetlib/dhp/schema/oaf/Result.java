package eu.dnetlib.dhp.schema.oaf;

import java.io.Serializable;
import java.util.Comparator;
import java.util.List;

public abstract class Result extends OafEntity implements Serializable {

    private List<Author> author;

    // resulttype allows subclassing results into publications | datasets | software
    private Qualifier resulttype;

    // common fields
    private Qualifier language;

    private List<Country> country;

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

    private Qualifier bestaccessright;

    private List<Context> context;

    private List<ExternalReference> externalReference;

    private List<Instance> instance;

    public List<Author> getAuthor() {
        return author;
    }

    public void setAuthor(List<Author> author) {
        this.author = author;
    }

    public Qualifier getResulttype() {
        return resulttype;
    }

    public void setResulttype(Qualifier resulttype) {
        this.resulttype = resulttype;
    }

    public Qualifier getLanguage() {
        return language;
    }

    public void setLanguage(Qualifier language) {
        this.language = language;
    }

    public List<Country> getCountry() {
        return country;
    }

    public void setCountry(List<Country> country) {
        this.country = country;
    }

    public List<StructuredProperty> getSubject() {
        return subject;
    }

    public void setSubject(List<StructuredProperty> subject) {
        this.subject = subject;
    }

    public List<StructuredProperty> getTitle() {
        return title;
    }

    public void setTitle(List<StructuredProperty> title) {
        this.title = title;
    }

    public List<StructuredProperty> getRelevantdate() {
        return relevantdate;
    }

    public void setRelevantdate(List<StructuredProperty> relevantdate) {
        this.relevantdate = relevantdate;
    }

    public List<Field<String>> getDescription() {
        return description;
    }

    public void setDescription(List<Field<String>> description) {
        this.description = description;
    }

    public Field<String> getDateofacceptance() {
        return dateofacceptance;
    }

    public void setDateofacceptance(Field<String> dateofacceptance) {
        this.dateofacceptance = dateofacceptance;
    }

    public Field<String> getPublisher() {
        return publisher;
    }

    public void setPublisher(Field<String> publisher) {
        this.publisher = publisher;
    }

    public Field<String> getEmbargoenddate() {
        return embargoenddate;
    }

    public void setEmbargoenddate(Field<String> embargoenddate) {
        this.embargoenddate = embargoenddate;
    }

    public List<Field<String>> getSource() {
        return source;
    }

    public void setSource(List<Field<String>> source) {
        this.source = source;
    }

    public List<Field<String>> getFulltext() {
        return fulltext;
    }

    public void setFulltext(List<Field<String>> fulltext) {
        this.fulltext = fulltext;
    }

    public List<Field<String>> getFormat() {
        return format;
    }

    public void setFormat(List<Field<String>> format) {
        this.format = format;
    }

    public List<Field<String>> getContributor() {
        return contributor;
    }

    public void setContributor(List<Field<String>> contributor) {
        this.contributor = contributor;
    }

    public Qualifier getResourcetype() {
        return resourcetype;
    }

    public void setResourcetype(Qualifier resourcetype) {
        this.resourcetype = resourcetype;
    }

    public List<Field<String>> getCoverage() {
        return coverage;
    }

    public void setCoverage(List<Field<String>> coverage) {
        this.coverage = coverage;
    }

    public Qualifier getBestaccessright() {
        return bestaccessright;
    }

    public void setBestaccessright(Qualifier bestaccessright) {
        this.bestaccessright = bestaccessright;
    }

    public List<Context> getContext() {
        return context;
    }

    public void setContext(List<Context> context) {
        this.context = context;
    }

    public List<ExternalReference> getExternalReference() {
        return externalReference;
    }

    public void setExternalReference(List<ExternalReference> externalReference) {
        this.externalReference = externalReference;
    }

    public List<Instance> getInstance() {
        return instance;
    }

    public void setInstance(List<Instance> instance) {
        this.instance = instance;
    }

    @Override
    public void mergeFrom(OafEntity e) {
        super.mergeFrom(e);

        Result r = (Result) e;

        instance = mergeLists(instance, r.getInstance());

        if (r.getResulttype() != null && compareTrust(this, r) < 0)
            resulttype = r.getResulttype();

        if (r.getLanguage() != null && compareTrust(this, r) < 0)
            language = r.getLanguage();

        country = mergeLists(country, r.getCountry());

        subject = mergeLists(subject, r.getSubject());

        title = mergeLists(title, r.getTitle());

        relevantdate = mergeLists(relevantdate, r.getRelevantdate());

        description = longestLists(description, r.getDescription());

        if (r.getPublisher() != null && compareTrust(this, r) < 0)
            publisher = r.getPublisher();

        if (r.getEmbargoenddate() != null && compareTrust(this, r) < 0)
            embargoenddate = r.getEmbargoenddate();

        source = mergeLists(source, r.getSource());

        fulltext = mergeLists(fulltext, r.getFulltext());

        format = mergeLists(format, r.getFormat());

        contributor = mergeLists(contributor, r.getContributor());

        if (r.getResourcetype() != null)
            resourcetype = r.getResourcetype();

        coverage = mergeLists(coverage, r.getCoverage());

        context = mergeLists(context, r.getContext());

        externalReference = mergeLists(externalReference, r.getExternalReference());
    }


    private List<Field<String>> longestLists(List<Field<String>> a, List<Field<String>> b) {
        if (a == null || b == null)
            return a == null ? b : a;
        if (a.size() == b.size()) {
            int msa = a.stream().filter(i -> i.getValue() != null).map(i -> i.getValue().length()).max(Comparator.naturalOrder()).orElse(0);
            int msb = b.stream().filter(i -> i.getValue() != null).map(i -> i.getValue().length()).max(Comparator.naturalOrder()).orElse(0);
            return msa > msb ? a : b;
        }
        return a.size() > b.size() ? a : b;
    }

}
