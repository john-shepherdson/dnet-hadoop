package eu.dnetlib.dhp.schema.dli;

import java.io.Serializable;
import java.util.List;

public class Entity implements Serializable {

    private String identifier;

    private List<Pid> pid;

    private List<String> title;

    private List<String> date;

    private String typology;

    private List<String> authors;

    private List<Subject> subject;

    private String description;

    private String completionStatus;

    private List<Provenance> collectedFrom;

    private List<String> publisher;


    public String getIdentifier() {
        return identifier;
    }

    public void setIdentifier(String identifier) {
        this.identifier = identifier;
    }

    public List<Pid> getPid() {
        return pid;
    }

    public void setPid(List<Pid> pid) {
        this.pid = pid;
    }

    public List<String> getTitle() {
        return title;
    }

    public void setTitle(List<String> title) {
        this.title = title;
    }

    public List<String> getDate() {
        return date;
    }

    public void setDate(List<String> date) {
        this.date = date;
    }

    public String getTypology() {
        return typology;
    }

    public void setTypology(String typology) {
        this.typology = typology;
    }

    public List<String> getAuthors() {
        return authors;
    }

    public void setAuthors(List<String> authors) {
        this.authors = authors;
    }

    public List<Subject> getSubject() {
        return subject;
    }

    public void setSubject(List<Subject> subject) {
        this.subject = subject;
    }

    public String getDescription() {
        return description;
    }

    public void setDescription(String description) {
        this.description = description;
    }

    public List<Provenance> getCollectedFrom() {
        return collectedFrom;
    }

    public void setCollectedFrom(List<Provenance> collectedFrom) {
        this.collectedFrom = collectedFrom;
    }

    public List<String> getPublisher() {
        return publisher;
    }

    public void setPublisher(List<String> publisher) {
        this.publisher = publisher;
    }

    public String getCompletionStatus() {
        return completionStatus;
    }

    public void setCompletionStatus(String completionStatus) {
        this.completionStatus = completionStatus;
    }
}
