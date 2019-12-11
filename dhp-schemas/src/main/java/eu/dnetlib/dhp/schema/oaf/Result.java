package eu.dnetlib.dhp.schema.oaf;

import org.apache.commons.lang3.StringUtils;

import java.io.Serializable;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.stream.Collectors;

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

    // ( article | book ) processing charges. Defined here to cope with possible wrongly typed results
    private Field<String> processingchargeamount;

    // currency - alphabetic code describe in ISO-4217. Defined here to cope with possible wrongly typed results
    private Field<String> processingchargecurrency;

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

    public List<Qualifier> getCountry() {
        return country;
    }

    public void setCountry(List<Qualifier> country) {
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

    public Field<String> getRefereed() {
        return refereed;
    }

    public void setRefereed(Field<String> refereed) {
        this.refereed = refereed;
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

    public Field<String> getProcessingchargeamount() {
        return processingchargeamount;
    }

    public Result setProcessingchargeamount(Field<String> processingchargeamount) {
        this.processingchargeamount = processingchargeamount;
        return this;
    }

    public Field<String> getProcessingchargecurrency() {
        return processingchargecurrency;
    }

    public Result setProcessingchargecurrency(Field<String> processingchargecurrency) {
        this.processingchargecurrency = processingchargecurrency;
        return this;
    }

    @Override
    public void mergeFrom(OafEntity e) {
        super.mergeFrom(e);

        Result r = (Result) e;



        //TODO mergeFrom is used only for create Dedup Records since the creation of these two fields requires more complex functions (maybe they will be filled in an external function)
//        dateofacceptance = r.getDateofacceptance();

        instance = mergeLists(instance, r.getInstance());

        if (r.getResulttype() != null)
            resulttype = r.getResulttype();

        if (r.getLanguage() != null)
            language = r.getLanguage();

        country = mergeLists(country, r.getCountry());

        subject = mergeLists(subject, r.getSubject());

        title = mergeLists(title, r.getTitle());

        relevantdate = mergeLists(relevantdate, r.getRelevantdate());

        description = mergeLists(description, r.getDescription());

        if (r.getPublisher() != null)
            publisher = r.getPublisher();

        if (r.getEmbargoenddate() != null)
            embargoenddate = r.getEmbargoenddate();

        source = mergeLists(source, r.getSource());

        fulltext = mergeLists(fulltext, r.getFulltext());

        format = mergeLists(format, r.getFormat());

        contributor = mergeLists(contributor, r.getContributor());

        if (r.getResourcetype() != null)
            resourcetype = r.getResourcetype();

        coverage = mergeLists(coverage, r.getCoverage());

        if (r.getRefereed() != null)
            refereed = r.getRefereed();

        context = mergeLists(context, r.getContext());

        if (r.getProcessingchargeamount() != null)
            processingchargeamount = r.getProcessingchargeamount();

        if (r.getProcessingchargecurrency() != null)
            processingchargecurrency = r.getProcessingchargecurrency();

        externalReference = mergeLists(externalReference, r.getExternalReference());

    }


}
