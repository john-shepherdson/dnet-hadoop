package eu.dnetlib.dhp.provision.scholix;

import java.io.Serializable;
import java.util.List;

public class ScholixResource implements Serializable {

    private ScholixIdentifier identifier ;
    private String dnetIdentifier ;
    private String objectType ;
    private String objectSubType ;
    private String title ;
    private List<ScholixEntityId> creator ;
    private String publicationDate ;
    private List<ScholixEntityId> publisher ;
    private List<ScholixCollectedFrom> collectedFrom ;


    public ScholixIdentifier getIdentifier() {
        return identifier;
    }

    public ScholixResource setIdentifier(ScholixIdentifier identifier) {
        this.identifier = identifier;
        return this;
    }

    public String getDnetIdentifier() {
        return dnetIdentifier;
    }

    public ScholixResource setDnetIdentifier(String dnetIdentifier) {
        this.dnetIdentifier = dnetIdentifier;
        return this;
    }

    public String getObjectType() {
        return objectType;
    }

    public ScholixResource setObjectType(String objectType) {
        this.objectType = objectType;
        return this;
    }

    public String getObjectSubType() {
        return objectSubType;
    }

    public ScholixResource setObjectSubType(String objectSubType) {
        this.objectSubType = objectSubType;
        return this;
    }

    public String getTitle() {
        return title;
    }

    public ScholixResource setTitle(String title) {
        this.title = title;
        return this;
    }

    public List<ScholixEntityId> getCreator() {
        return creator;
    }

    public ScholixResource setCreator(List<ScholixEntityId> creator) {
        this.creator = creator;
        return this;
    }

    public String getPublicationDate() {
        return publicationDate;
    }

    public ScholixResource setPublicationDate(String publicationDate) {
        this.publicationDate = publicationDate;
        return this;
    }

    public List<ScholixEntityId> getPublisher() {
        return publisher;
    }

    public ScholixResource setPublisher(List<ScholixEntityId> publisher) {
        this.publisher = publisher;
        return this;
    }

    public List<ScholixCollectedFrom> getCollectedFrom() {
        return collectedFrom;
    }

    public ScholixResource setCollectedFrom(List<ScholixCollectedFrom> collectedFrom) {
        this.collectedFrom = collectedFrom;
        return this;
    }
}
