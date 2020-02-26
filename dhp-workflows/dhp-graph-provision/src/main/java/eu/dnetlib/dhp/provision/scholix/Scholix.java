package eu.dnetlib.dhp.provision.scholix;

import com.fasterxml.jackson.databind.ObjectMapper;
import eu.dnetlib.dhp.provision.scholix.summary.ScholixSummary;
import eu.dnetlib.dhp.schema.oaf.Relation;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

public class Scholix implements Serializable {
    private String publicationDate;

    private List<ScholixEntityId> publisher;

    private List<ScholixEntityId> linkprovider;

    private ScholixRelationship relationship;

    private ScholixResource source;

    private ScholixResource target;

    private String identifier;


    public static Scholix generateScholixWithSource(final String sourceSummaryJson, final String relation) {
        final ObjectMapper mapper = new ObjectMapper();

        try {
            ScholixSummary scholixSummary = mapper.readValue(sourceSummaryJson, ScholixSummary.class);
            Relation rel = mapper.readValue(sourceSummaryJson, Relation.class);
            final Scholix s = new Scholix();
            if (scholixSummary.getDate() != null)
                s.setPublicationDate(scholixSummary.getDate().stream().findFirst().orElse(null));


            s.setLinkprovider(rel.getCollectedFrom().stream().map(cf ->
                    new ScholixEntityId(cf.getValue(), Collections.singletonList(
                            new ScholixIdentifier(cf.getKey(), "dnet_identifier")
                    ))).collect(Collectors.toList()));


        } catch (Throwable e) {
            throw new RuntimeException(e);
        }
    }

    public Scholix addTarget(final String targetSummaryJson) {
        return this;
    }


    public String getPublicationDate() {
        return publicationDate;
    }

    public Scholix setPublicationDate(String publicationDate) {
        this.publicationDate = publicationDate;
        return this;
    }

    public List<ScholixEntityId> getPublisher() {
        return publisher;
    }

    public Scholix setPublisher(List<ScholixEntityId> publisher) {
        this.publisher = publisher;
        return this;
    }

    public List<ScholixEntityId> getLinkprovider() {
        return linkprovider;
    }

    public Scholix setLinkprovider(List<ScholixEntityId> linkprovider) {
        this.linkprovider = linkprovider;
        return this;
    }

    public ScholixRelationship getRelationship() {
        return relationship;
    }

    public Scholix setRelationship(ScholixRelationship relationship) {
        this.relationship = relationship;
        return this;
    }

    public ScholixResource getSource() {
        return source;
    }

    public Scholix setSource(ScholixResource source) {
        this.source = source;
        return this;
    }

    public ScholixResource getTarget() {
        return target;
    }

    public Scholix setTarget(ScholixResource target) {
        this.target = target;
        return this;
    }

    public String getIdentifier() {
        return identifier;
    }

    public Scholix setIdentifier(String identifier) {
        this.identifier = identifier;
        return this;
    }
}
