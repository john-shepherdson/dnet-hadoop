
package eu.dnetlib.dhp.provision.scholix;

import java.io.Serializable;
import java.util.*;
import java.util.stream.Collectors;

import org.apache.commons.lang3.StringUtils;

import com.fasterxml.jackson.databind.ObjectMapper;

import eu.dnetlib.dhp.provision.scholix.summary.ScholixSummary;
import eu.dnetlib.dhp.schema.oaf.Relation;
import eu.dnetlib.dhp.utils.DHPUtils;

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
			ScholixSummary scholixSummary = mapper.readValue(sourceSummaryJson, ScholixSummary.class);
			Relation rel = mapper.readValue(relation, Relation.class);
			final Scholix s = new Scholix();
			if (scholixSummary.getDate() != null && scholixSummary.getDate().size() > 0)
				s.setPublicationDate(scholixSummary.getDate().get(0));
			s
				.setLinkprovider(
					rel
						.getCollectedfrom()
						.stream()
						.map(
							cf -> new ScholixEntityId(
								cf.getValue(),
								Collections
									.singletonList(
										new ScholixIdentifier(cf.getKey(), "dnet_identifier"))))
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
		s
			.setLinkprovider(
				rel
					.getCollectedfrom()
					.stream()
					.map(
						cf -> new ScholixEntityId(
							cf.getValue(),
							Collections
								.singletonList(
									new ScholixIdentifier(cf.getKey(), "dnet_identifier"))))
					.collect(Collectors.toList()));
		s.setRelationship(new ScholixRelationship(rel.getRelType(), rel.getRelClass(), null));
		s.setSource(ScholixResource.fromSummary(scholixSummary));

		s.setIdentifier(rel.getTarget());
		return s;
	}

	private List<ScholixEntityId> mergeScholixEntityId(final List<ScholixEntityId> a, final List<ScholixEntityId> b) {
		final List<ScholixEntityId> m = new ArrayList<>(a);
		if (b != null)
			b.forEach(s -> {
				int tt = (int) m.stream().filter(t -> t.getName().equalsIgnoreCase(s.getName())).count();
				if (tt == 0) {
					m.add(s);
				}
			});
		return m;
	}

	private List<ScholixIdentifier> mergeScholixIdnetifier(final List<ScholixIdentifier> a,
		final List<ScholixIdentifier> b) {
		final List<ScholixIdentifier> m = new ArrayList<>(a);
		if (b != null)
			b.forEach(s -> {
				int tt = (int) m.stream().filter(t -> t.getIdentifier().equalsIgnoreCase(s.getIdentifier())).count();
				if (tt == 0) {
					m.add(s);
				}
			});
		return m;
	}

	private List<ScholixCollectedFrom> mergeScholixCollectedFrom(final List<ScholixCollectedFrom> a,
		final List<ScholixCollectedFrom> b) {
		final List<ScholixCollectedFrom> m = new ArrayList<>(a);
		if (b != null)
			b.forEach(s -> {
				int tt = (int) m
					.stream()
					.filter(t -> t.getProvider().getName().equalsIgnoreCase(s.getProvider().getName()))
					.count();
				if (tt == 0) {
					m.add(s);
				}
			});
		return m;
	}

	private ScholixRelationship mergeRelationships(final ScholixRelationship a, final ScholixRelationship b) {
		ScholixRelationship result = new ScholixRelationship();
		result.setName(StringUtils.isEmpty(a.getName()) ? b.getName() : a.getName());
		result.setInverse(StringUtils.isEmpty(a.getInverse()) ? b.getInverse() : a.getInverse());
		result.setSchema(StringUtils.isEmpty(a.getSchema()) ? b.getSchema() : a.getSchema());
		return result;
	}

	private ScholixResource mergeResource(final ScholixResource a, final ScholixResource b) {

		final ScholixResource result = new ScholixResource();
		result.setCollectedFrom(mergeScholixCollectedFrom(a.getCollectedFrom(), b.getCollectedFrom()));
		result.setCreator(mergeScholixEntityId(a.getCreator(), b.getCreator()));
		result
			.setDnetIdentifier(
				StringUtils.isBlank(a.getDnetIdentifier()) ? b.getDnetIdentifier() : a.getDnetIdentifier());
		result.setIdentifier(mergeScholixIdnetifier(a.getIdentifier(), b.getIdentifier()));
		result.setObjectType(StringUtils.isNotBlank(a.getObjectType()) ? a.getObjectType() : b.getObjectType());
		result
			.setObjectSubType(
				StringUtils.isNotBlank(a.getObjectSubType()) ? a.getObjectSubType() : b.getObjectSubType());
		result.setPublisher(mergeScholixEntityId(a.getPublisher(), b.getPublisher()));
		result
			.setPublicationDate(
				StringUtils.isNotBlank(a.getPublicationDate()) ? a.getPublicationDate() : b.getPublicationDate());
		result.setTitle(StringUtils.isNotBlank(a.getTitle()) ? a.getTitle() : b.getTitle());
		return result;

	}

	public void mergeFrom(final Scholix other) {
		linkprovider = mergeScholixEntityId(linkprovider, other.getLinkprovider());
		publisher = mergeScholixEntityId(publisher, other.getPublisher());
		if (StringUtils.isEmpty(publicationDate))
			publicationDate = other.getPublicationDate();
		relationship = mergeRelationships(relationship, other.getRelationship());
		source = mergeResource(source, other.getSource());
		target = mergeResource(target, other.getTarget());
		generateIdentifier();
	}

	public void generatelinkPublisher() {
		Set<String> publisher = new HashSet<>();
		if (source.getPublisher() != null)
			publisher
				.addAll(
					source
						.getPublisher()
						.stream()
						.map(ScholixEntityId::getName)
						.collect(Collectors.toList()));
		if (target.getPublisher() != null)
			publisher
				.addAll(
					target
						.getPublisher()
						.stream()
						.map(ScholixEntityId::getName)
						.collect(Collectors.toList()));
		this.publisher = publisher.stream().map(k -> new ScholixEntityId(k, null)).collect(Collectors.toList());
	}

	public void generateIdentifier() {
		setIdentifier(
			DHPUtils
				.md5(
					String
						.format(
							"%s::%s::%s",
							source.getDnetIdentifier(), relationship.getName(), target.getDnetIdentifier())));
	}

	public Scholix addTarget(final String targetSummaryJson) {
		final ObjectMapper mapper = new ObjectMapper();

		try {
			ScholixSummary targetSummary = mapper.readValue(targetSummaryJson, ScholixSummary.class);
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
