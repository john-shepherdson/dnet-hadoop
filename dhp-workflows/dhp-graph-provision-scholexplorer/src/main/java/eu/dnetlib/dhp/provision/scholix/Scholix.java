package eu.dnetlib.dhp.provision.scholix;

import com.fasterxml.jackson.databind.ObjectMapper;
import eu.dnetlib.dhp.provision.scholix.summary.ScholixSummary;
import eu.dnetlib.dhp.schema.oaf.Relation;
import eu.dnetlib.dhp.utils.DHPUtils;
import java.io.Serializable;
import java.util.*;
import java.util.stream.Collectors;

public class Scholix implements Serializable {
    private String publicationDate;

    private List<ScholixEntityId> publisher;

    private List<ScholixEntityId> linkprovider;

    private ScholixRelationship relationship;

    private ScholixResource source;

    private ScholixResource target;

    private String identifier;

    public Scholix clone(final ScholixResource t) {
        final Scholix clone = new Scholix();
        clone.setPublicationDate(publicationDate);
        clone.setPublisher(publisher);
        clone.setLinkprovider(linkprovider);
        clone.setRelationship(relationship);
        clone.setSource(source);
        clone.setTarget(t);
        clone.generatelinkPublisher();
        clone.generateIdentifier();
        return clone;
    }

    public static Scholix generateScholixWithSource(
            final String sourceSummaryJson, final String relation) {
        final ObjectMapper mapper = new ObjectMapper();

        try {
            ScholixSummary scholixSummary =
                    mapper.readValue(sourceSummaryJson, ScholixSummary.class);
            Relation rel = mapper.readValue(relation, Relation.class);
            final Scholix s = new Scholix();
            if (scholixSummary.getDate() != null && scholixSummary.getDate().size() > 0)
                s.setPublicationDate(scholixSummary.getDate().get(0));
            s.setLinkprovider(
                    rel.getCollectedFrom().stream()
                            .map(
                                    cf ->
                                            new ScholixEntityId(
                                                    cf.getValue(),
                                                    Collections.singletonList(
                                                            new ScholixIdentifier(
                                                                    cf.getKey(),
                                                                    "dnet_identifier"))))
                            .collect(Collectors.toList()));
            s.setRelationship(new ScholixRelationship(rel.getRelType(), rel.getRelClass(), null));
            s.setSource(ScholixResource.fromSummary(scholixSummary));
            return s;
        } catch (Throwable e) {
            throw new RuntimeException(
                    String.format("Summary: %s \n relation:%s", sourceSummaryJson, relation), e);
        }
    }

    public static Scholix generateScholixWithSource(
            final ScholixSummary scholixSummary, final Relation rel) {
        final Scholix s = new Scholix();
        if (scholixSummary.getDate() != null && scholixSummary.getDate().size() > 0)
            s.setPublicationDate(scholixSummary.getDate().get(0));
        s.setLinkprovider(
                rel.getCollectedFrom().stream()
                        .map(
                                cf ->
                                        new ScholixEntityId(
                                                cf.getValue(),
                                                Collections.singletonList(
                                                        new ScholixIdentifier(
                                                                cf.getKey(), "dnet_identifier"))))
                        .collect(Collectors.toList()));
        s.setRelationship(new ScholixRelationship(rel.getRelType(), rel.getRelClass(), null));
        s.setSource(ScholixResource.fromSummary(scholixSummary));

        s.setIdentifier(rel.getTarget());
        //        ScholixResource mockTarget = new ScholixResource();
        //        mockTarget.setDnetIdentifier(rel.getTarget());
        //        s.setTarget(mockTarget);
        //        s.generateIdentifier();
        return s;
    }

    public void generatelinkPublisher() {
        Set<String> publisher = new HashSet<>();
        if (source.getPublisher() != null)
            publisher.addAll(
                    source.getPublisher().stream()
                            .map(ScholixEntityId::getName)
                            .collect(Collectors.toList()));
        if (target.getPublisher() != null)
            publisher.addAll(
                    target.getPublisher().stream()
                            .map(ScholixEntityId::getName)
                            .collect(Collectors.toList()));
        this.publisher =
                publisher.stream()
                        .map(k -> new ScholixEntityId(k, null))
                        .collect(Collectors.toList());
    }

    public void generateIdentifier() {
        setIdentifier(
                DHPUtils.md5(
                        String.format(
                                "%s::%s::%s",
                                source.getDnetIdentifier(),
                                relationship.getName(),
                                target.getDnetIdentifier())));
    }

    public Scholix addTarget(final String targetSummaryJson) {
        final ObjectMapper mapper = new ObjectMapper();

        try {
            ScholixSummary targetSummary =
                    mapper.readValue(targetSummaryJson, ScholixSummary.class);
            setTarget(ScholixResource.fromSummary(targetSummary));
            generateIdentifier();
            return this;
        } catch (Throwable e) {
            throw new RuntimeException(e);
        }
    }

    public String getPublicationDate() {
        return publicationDate;
    }

    public void setPublicationDate(String publicationDate) {
        this.publicationDate = publicationDate;
    }

    public List<ScholixEntityId> getPublisher() {
        return publisher;
    }

    public void setPublisher(List<ScholixEntityId> publisher) {
        this.publisher = publisher;
    }

    public List<ScholixEntityId> getLinkprovider() {
        return linkprovider;
    }

    public void setLinkprovider(List<ScholixEntityId> linkprovider) {
        this.linkprovider = linkprovider;
    }

    public ScholixRelationship getRelationship() {
        return relationship;
    }

    public void setRelationship(ScholixRelationship relationship) {
        this.relationship = relationship;
    }

    public ScholixResource getSource() {
        return source;
    }

    public void setSource(ScholixResource source) {
        this.source = source;
    }

    public ScholixResource getTarget() {
        return target;
    }

    public void setTarget(ScholixResource target) {
        this.target = target;
    }

    public String getIdentifier() {
        return identifier;
    }

    public void setIdentifier(String identifier) {
        this.identifier = identifier;
    }
}
