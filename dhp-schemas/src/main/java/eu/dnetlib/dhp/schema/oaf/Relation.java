package eu.dnetlib.dhp.schema.oaf;

import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static com.google.common.base.Preconditions.checkArgument;

/**
 * Relation aims to model any edge between two nodes in the OpenAIRE graph. It has a source id and a target id
 * pointing to graph node identifiers and it is further characterised by the semantic of the link throught the fields
 * relType, subRelType and relClass. Provenance information is modeled according to the dataInfo element and collectedFrom,
 * while individual relationship types can provide extra information via the properties field.
 */
public class Relation extends Oaf {

	/**
	 * Main relationship classifier, values include 'resultResult', 'resultProject', 'resultOrganization', etc.
	 */
	private String relType;

	/**
	 * Further classifies a relationship, values include 'affiliation', 'similarity', 'supplement', etc.
	 */
	private String subRelType;

	/**
	 * Indicates the direction of the relationship, values include 'isSupplementTo', 'isSupplementedBy', 'merges, 'isMergedIn'.
	 */
	private String relClass;

	/**
	 * The source entity id.
	 */
	private String source;

	/**
	 * The target entity id.
	 */
	private String target;

	/**
	 * The list of datasource id/name pairs providing this relationship.
	 */
	private List<KeyValue> collectedFrom = new ArrayList<>();

	/**
	 * List of relation specific properties. Values include 'similarityLevel', indicating the similarity score between a pair of publications.
	 */
	private List<KeyValue> properties = new ArrayList<>();

	public String getRelType() {
		return relType;
	}

	public void setRelType(final String relType) {
		this.relType = relType;
	}

	public String getSubRelType() {
		return subRelType;
	}

	public void setSubRelType(final String subRelType) {
		this.subRelType = subRelType;
	}

	public String getRelClass() {
		return relClass;
	}

	public void setRelClass(final String relClass) {
		this.relClass = relClass;
	}

	public String getSource() {
		return source;
	}

	public void setSource(final String source) {
		this.source = source;
	}

	public String getTarget() {
		return target;
	}

	public void setTarget(final String target) {
		this.target = target;
	}

	public List<KeyValue> getCollectedFrom() {
		return collectedFrom;
	}

	public void setCollectedFrom(final List<KeyValue> collectedFrom) {
		this.collectedFrom = collectedFrom;
	}

	public List<KeyValue> getProperties() {
		return properties;
	}

	public void setProperties(List<KeyValue> properties) {
		this.properties = properties;
	}

	public void mergeFrom(final Relation r) {

		checkArgument(Objects.equals(getSource(), r.getSource()),"source ids must be equal");
		checkArgument(Objects.equals(getTarget(), r.getTarget()),"target ids must be equal");
		checkArgument(Objects.equals(getRelType(), r.getRelType()),"relType(s) must be equal");
		checkArgument(Objects.equals(getSubRelType(), r.getSubRelType()),"subRelType(s) must be equal");
		checkArgument(Objects.equals(getRelClass(), r.getRelClass()),"relClass(es) must be equal");

		setCollectedFrom(
				Stream
						.concat(Optional.ofNullable(getCollectedFrom()).map(Collection::stream).orElse(Stream.empty()),
								Optional.ofNullable(r.getCollectedFrom()).map(Collection::stream).orElse(Stream.empty()))
						.distinct() // relies on KeyValue.equals
						.collect(Collectors.toList()));
	}

	@Override
	public boolean equals(Object o) {
		if (this == o) return true;
		if (o == null || getClass() != o.getClass()) return false;
		Relation relation = (Relation) o;
		return relType.equals(relation.relType) &&
				subRelType.equals(relation.subRelType) &&
				relClass.equals(relation.relClass) &&
				source.equals(relation.source) &&
				target.equals(relation.target);
	}

	@Override
	public int hashCode() {
		return Objects.hash(relType, subRelType, relClass, source, target, collectedFrom);
	}

}
