
package eu.dnetlib.dhp.schema.oaf;

import eu.dnetlib.dhp.schema.common.ModelSupport;

import static com.google.common.base.Preconditions.checkArgument;

import java.text.ParseException;
import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * Relation models any edge between two nodes in the OpenAIRE graph. It has a source id and a target id pointing to
 * graph node identifiers and it is further characterised by the semantic of the link through the fields relType,
 * subRelType and relClass. Provenance information is modeled according to the dataInfo element and collectedFrom, while
 * individual relationship types can provide extra information via the properties field.
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
	 * Indicates the direction of the relationship, values include 'isSupplementTo', 'isSupplementedBy', 'merges,
	 * 'isMergedIn'.
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
	 * Was this relationship authoritatively validated?
	 */
	private Boolean validated;

	/**
	 * When was this relationship authoritatively validated.
	 */
	private String validationDate;

	/**
	 * List of relation specific properties. Values include 'similarityLevel', indicating the similarity score between a
	 * pair of publications.
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

	public List<KeyValue> getProperties() {
		return properties;
	}

	public void setProperties(List<KeyValue> properties) {
		this.properties = properties;
	}

	public Boolean getValidated() {
		return Objects.nonNull(validated) && validated;
	}

	public void setValidated(Boolean validated) {
		this.validated = validated;
	}

	public String getValidationDate() {
		return validationDate;
	}

	public void setValidationDate(String validationDate) {
		this.validationDate = validationDate;
	}

	public void mergeFrom(final Relation r) {

		checkArgument(Objects.equals(getSource(), r.getSource()), "source ids must be equal");
		checkArgument(Objects.equals(getTarget(), r.getTarget()), "target ids must be equal");
		checkArgument(Objects.equals(getRelType(), r.getRelType()), "relType(s) must be equal");
		checkArgument(
			Objects.equals(getSubRelType(), r.getSubRelType()), "subRelType(s) must be equal");
		checkArgument(Objects.equals(getRelClass(), r.getRelClass()), "relClass(es) must be equal");

		setValidated(getValidated() || r.getValidated());
		try {
			setValidationDate(ModelSupport.oldest(getValidationDate(), r.getValidationDate()));
		} catch (ParseException e) {
			throw new IllegalArgumentException(String.format("invalid validation date format in relation [s:%s, t:%s]: %s", getSource(), getTarget(), getValidationDate()));
		}

		super.mergeFrom(r);
	}

	@Override
	public boolean equals(Object o) {
		if (this == o)
			return true;
		if (o == null || getClass() != o.getClass())
			return false;
		Relation relation = (Relation) o;
		return relType.equals(relation.relType)
			&& subRelType.equals(relation.subRelType)
			&& relClass.equals(relation.relClass)
			&& source.equals(relation.source)
			&& target.equals(relation.target);
	}

	@Override
	public int hashCode() {
		return Objects.hash(relType, subRelType, relClass, source, target, collectedfrom);
	}

}
