
package eu.dnetlib.dhp.provision.scholix.summary;

import java.io.Serializable;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;

import eu.dnetlib.dhp.provision.RelatedItemInfo;
import eu.dnetlib.dhp.schema.oaf.Author;
import eu.dnetlib.dhp.schema.oaf.Oaf;
import eu.dnetlib.dhp.schema.oaf.OafEntity;
import eu.dnetlib.dhp.schema.oaf.StructuredProperty;
import eu.dnetlib.dhp.schema.scholexplorer.DLIDataset;
import eu.dnetlib.dhp.schema.scholexplorer.DLIPublication;
import eu.dnetlib.dhp.schema.scholexplorer.DLIUnknown;

public class ScholixSummary implements Serializable {
	private String id;
	private List<TypedIdentifier> localIdentifier;
	private Typology typology;
	private List<String> title;
	private List<String> author;
	private List<String> date;
	private String description;
	private List<SchemeValue> subject;
	private List<String> publisher;
	private long relatedPublications;
	private long relatedDatasets;
	private long relatedUnknown;
	private List<CollectedFromType> datasources;

	public String getId() {
		return id;
	}

	public void setId(String id) {
		this.id = id;
	}

	public List<TypedIdentifier> getLocalIdentifier() {
		return localIdentifier;
	}

	public void setLocalIdentifier(List<TypedIdentifier> localIdentifier) {
		this.localIdentifier = localIdentifier;
	}

	public Typology getTypology() {
		return typology;
	}

	public void setTypology(Typology typology) {
		this.typology = typology;
	}

	public List<String> getTitle() {
		return title;
	}

	public void setTitle(List<String> title) {
		this.title = title;
	}

	public List<String> getAuthor() {
		return author;
	}

	public void setAuthor(List<String> author) {
		this.author = author;
	}

	public List<String> getDate() {
		return date;
	}

	public void setDate(List<String> date) {
		this.date = date;
	}

	@JsonProperty("abstract")
	public String getDescription() {
		return description;
	}

	@JsonProperty("abstract")
	public void setDescription(String description) {
		this.description = description;
	}

	public List<SchemeValue> getSubject() {
		return subject;
	}

	public void setSubject(List<SchemeValue> subject) {
		this.subject = subject;
	}

	public List<String> getPublisher() {
		return publisher;
	}

	public void setPublisher(List<String> publisher) {
		this.publisher = publisher;
	}

	public long getRelatedPublications() {
		return relatedPublications;
	}

	public void setRelatedPublications(long relatedPublications) {
		this.relatedPublications = relatedPublications;
	}

	public long getRelatedDatasets() {
		return relatedDatasets;
	}

	public void setRelatedDatasets(long relatedDatasets) {
		this.relatedDatasets = relatedDatasets;
	}

	public long getRelatedUnknown() {
		return relatedUnknown;
	}

	public void setRelatedUnknown(long relatedUnknown) {
		this.relatedUnknown = relatedUnknown;
	}

	public List<CollectedFromType> getDatasources() {
		return datasources;
	}

	public void setDatasources(List<CollectedFromType> datasources) {
		this.datasources = datasources;
	}

	public static ScholixSummary fromOAF(final Oaf oaf) {
		try {
			final RelatedItemInfo relatedItemInfo = new RelatedItemInfo();

			if (oaf instanceof DLIPublication)
				return summaryFromPublication((DLIPublication) oaf, relatedItemInfo);
			if (oaf instanceof DLIDataset)
				return summaryFromDataset((DLIDataset) oaf, relatedItemInfo);
			if (oaf instanceof DLIUnknown)
				return summaryFromUnknown((DLIUnknown) oaf, relatedItemInfo);

		} catch (Throwable e) {
			throw new RuntimeException(e);
		}
		return null;
	}

	private static ScholixSummary summaryFromDataset(
		final DLIDataset item, final RelatedItemInfo relatedItemInfo) {
		ScholixSummary summary = new ScholixSummary();
		summary.setId(item.getId());

		if (item.getPid() != null)
			summary
				.setLocalIdentifier(
					item
						.getPid()
						.stream()
						.map(p -> new TypedIdentifier(p.getValue(), p.getQualifier().getClassid()))
						.collect(Collectors.toList()));

		summary.setTypology(Typology.dataset);
		if (item.getTitle() != null)
			summary
				.setTitle(
					item.getTitle().stream().map(StructuredProperty::getValue).collect(Collectors.toList()));

		if (item.getAuthor() != null) {
			summary
				.setAuthor(
					item.getAuthor().stream().map(Author::getFullname).collect(Collectors.toList()));
		}

		if (item.getRelevantdate() != null)
			summary
				.setDate(
					item
						.getRelevantdate()
						.stream()
						.filter(d -> "date".equalsIgnoreCase(d.getQualifier().getClassname()))
						.map(StructuredProperty::getValue)
						.collect(Collectors.toList()));

		if (item.getDescription() != null && item.getDescription().size() > 0)
			summary.setDescription(item.getDescription().get(0).getValue());

		if (item.getSubject() != null) {
			summary
				.setSubject(
					item
						.getSubject()
						.stream()
						.map(s -> new SchemeValue(s.getQualifier().getClassid(), s.getValue()))
						.collect(Collectors.toList()));
		}
		if (item.getPublisher() != null)
			summary.setPublisher(Collections.singletonList(item.getPublisher().getValue()));

		summary.setRelatedDatasets(relatedItemInfo.getRelatedDataset());
		summary.setRelatedPublications(relatedItemInfo.getRelatedPublication());
		summary.setRelatedUnknown(relatedItemInfo.getRelatedUnknown());

		if (item.getDlicollectedfrom() != null)
			summary
				.setDatasources(
					item
						.getDlicollectedfrom()
						.stream()
						.map(c -> new CollectedFromType(c.getName(), c.getId(), c.getCompletionStatus()))
						.collect(Collectors.toList()));
		return summary;
	}

	private static ScholixSummary summaryFromPublication(
		final DLIPublication item, final RelatedItemInfo relatedItemInfo) {
		ScholixSummary summary = new ScholixSummary();
		summary.setId(item.getId());

		if (item.getPid() != null)
			summary
				.setLocalIdentifier(
					item
						.getPid()
						.stream()
						.map(p -> new TypedIdentifier(p.getValue(), p.getQualifier().getClassid()))
						.collect(Collectors.toList()));

		summary.setTypology(Typology.publication);
		if (item.getTitle() != null)
			summary
				.setTitle(
					item.getTitle().stream().map(StructuredProperty::getValue).collect(Collectors.toList()));

		if (item.getAuthor() != null) {
			summary
				.setAuthor(
					item.getAuthor().stream().map(Author::getFullname).collect(Collectors.toList()));
		}

		if (item.getRelevantdate() != null)
			summary
				.setDate(
					item
						.getRelevantdate()
						.stream()
						.filter(d -> "date".equalsIgnoreCase(d.getQualifier().getClassname()))
						.map(StructuredProperty::getValue)
						.collect(Collectors.toList()));

		if (item.getDescription() != null && item.getDescription().size() > 0)
			summary.setDescription(item.getDescription().get(0).getValue());

		if (item.getSubject() != null) {
			summary
				.setSubject(
					item
						.getSubject()
						.stream()
						.map(s -> new SchemeValue(s.getQualifier().getClassid(), s.getValue()))
						.collect(Collectors.toList()));
		}

		if (item.getPublisher() != null)
			summary.setPublisher(Collections.singletonList(item.getPublisher().getValue()));

		summary.setRelatedDatasets(relatedItemInfo.getRelatedDataset());
		summary.setRelatedPublications(relatedItemInfo.getRelatedPublication());
		summary.setRelatedUnknown(relatedItemInfo.getRelatedUnknown());

		if (item.getDlicollectedfrom() != null)
			summary
				.setDatasources(
					item
						.getDlicollectedfrom()
						.stream()
						.map(c -> new CollectedFromType(c.getName(), c.getId(), c.getCompletionStatus()))
						.collect(Collectors.toList()));

		return summary;
	}

	private static ScholixSummary summaryFromUnknown(
		final DLIUnknown item, final RelatedItemInfo relatedItemInfo) {
		ScholixSummary summary = new ScholixSummary();
		summary.setId(item.getId());
		if (item.getPid() != null)
			summary
				.setLocalIdentifier(
					item
						.getPid()
						.stream()
						.map(p -> new TypedIdentifier(p.getValue(), p.getQualifier().getClassid()))
						.collect(Collectors.toList()));

		summary.setRelatedDatasets(relatedItemInfo.getRelatedDataset());
		summary.setRelatedPublications(relatedItemInfo.getRelatedPublication());
		summary.setRelatedUnknown(relatedItemInfo.getRelatedUnknown());
		summary.setTypology(Typology.unknown);
		if (item.getDlicollectedfrom() != null)
			summary
				.setDatasources(
					item
						.getDlicollectedfrom()
						.stream()
						.map(c -> new CollectedFromType(c.getName(), c.getId(), c.getCompletionStatus()))
						.collect(Collectors.toList()));
		return summary;
	}
}
