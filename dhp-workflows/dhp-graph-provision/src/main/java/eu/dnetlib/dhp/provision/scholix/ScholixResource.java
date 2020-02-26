package eu.dnetlib.dhp.provision.scholix;

import eu.dnetlib.dhp.provision.scholix.summary.ScholixSummary;

import java.io.Serializable;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

public class ScholixResource implements Serializable {

    private List<ScholixIdentifier> identifier;
    private String dnetIdentifier;
    private String objectType;
    private String objectSubType;
    private String title;
    private List<ScholixEntityId> creator;
    private String publicationDate;
    private List<ScholixEntityId> publisher;
    private List<ScholixCollectedFrom> collectedFrom;


    public static ScholixResource fromSummary(ScholixSummary summary) {

        final ScholixResource resource = new ScholixResource();

        resource.setDnetIdentifier(summary.getId());

        resource.setIdentifier(summary.getLocalIdentifier().stream()
                .map(i ->
                        new ScholixIdentifier(i.getId(), i.getType()))
                .collect(Collectors.toList()));

        resource.setObjectType(summary.getTypology().toString());

        resource.setTitle(summary.getTitle().stream().findAny().orElse(null));

        if (summary.getAuthor() != null)
            resource.setCreator(summary.getAuthor().stream()
                    .map(c -> new ScholixEntityId(c, null))
                    .collect(Collectors.toList())
            );

        if (summary.getDate() != null)
            resource.setPublicationDate(summary.getDate().stream().findAny().orElse(null));
        if (summary.getPublisher() != null)
            resource.setPublisher(summary.getPublisher().stream()
                    .map(p -> new ScholixEntityId(p, null))
                    .collect(Collectors.toList())
            );
        if (summary.getDatasources() != null)
            resource.setCollectedFrom(summary.getDatasources().stream()
                    .map(d ->
                            new ScholixCollectedFrom(new ScholixEntityId(d.getDatasourceName(),
                            Collections.singletonList(new ScholixIdentifier(d.getDatasourceId(), "dnet_identifier"))
                    ), "collected", d.getCompletionStatus()))
                    .collect(Collectors.toList()));
        return resource;

    }

    public List<ScholixIdentifier> getIdentifier() {
        return identifier;
    }

    public ScholixResource setIdentifier(List<ScholixIdentifier> identifier) {
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
