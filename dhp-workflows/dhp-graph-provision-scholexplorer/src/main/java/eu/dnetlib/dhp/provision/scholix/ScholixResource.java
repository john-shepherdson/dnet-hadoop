
package eu.dnetlib.dhp.provision.scholix;

import eu.dnetlib.dhp.provision.scholix.summary.ScholixSummary;
import java.io.Serializable;
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

		resource
			.setIdentifier(
				summary
					.getLocalIdentifier()
					.stream()
					.map(i -> new ScholixIdentifier(i.getId(), i.getType()))
					.collect(Collectors.toList()));

		resource.setObjectType(summary.getTypology().toString());

		if (summary.getTitle() != null && summary.getTitle().size() > 0)
			resource.setTitle(summary.getTitle().get(0));

		if (summary.getAuthor() != null)
			resource
				.setCreator(
					summary
						.getAuthor()
						.stream()
						.map(c -> new ScholixEntityId(c, null))
						.collect(Collectors.toList()));

		if (summary.getDate() != null && summary.getDate().size() > 0)
			resource.setPublicationDate(summary.getDate().get(0));
		if (summary.getPublisher() != null)
			resource
				.setPublisher(
					summary
						.getPublisher()
						.stream()
						.map(p -> new ScholixEntityId(p, null))
						.collect(Collectors.toList()));
		if (summary.getDatasources() != null)
			resource
				.setCollectedFrom(
					summary
						.getDatasources()
						.stream()
						.map(
							d -> new ScholixCollectedFrom(
								new ScholixEntityId(
									d.getDatasourceName(),
									Collections
										.singletonList(
											new ScholixIdentifier(d.getDatasourceId(), "dnet_identifier"))),
								"collected",
								d.getCompletionStatus()))
						.collect(Collectors.toList()));
		return resource;
	}

	public List<ScholixIdentifier> getIdentifier() {
		return identifier;
	}

	public void setIdentifier(List<ScholixIdentifier> identifier) {
		this.identifier = identifier;
	}

	public String getDnetIdentifier() {
		return dnetIdentifier;
	}

	public void setDnetIdentifier(String dnetIdentifier) {
		this.dnetIdentifier = dnetIdentifier;
	}

	public String getObjectType() {
		return objectType;
	}

	public void setObjectType(String objectType) {
		this.objectType = objectType;
	}

	public String getObjectSubType() {
		return objectSubType;
	}

	public void setObjectSubType(String objectSubType) {
		this.objectSubType = objectSubType;
	}

	public String getTitle() {
		return title;
	}

	public void setTitle(String title) {
		this.title = title;
	}

	public List<ScholixEntityId> getCreator() {
		return creator;
	}

	public void setCreator(List<ScholixEntityId> creator) {
		this.creator = creator;
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

	public List<ScholixCollectedFrom> getCollectedFrom() {
		return collectedFrom;
	}

	public void setCollectedFrom(List<ScholixCollectedFrom> collectedFrom) {
		this.collectedFrom = collectedFrom;
	}
}
