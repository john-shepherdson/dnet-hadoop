package eu.dnetlib.dhp.schema.oaf;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static com.google.common.base.Preconditions.checkArgument;

public class Relation extends Oaf {

	private String relType;

	private String subRelType;

	private String relClass;

	private String source;

	private String target;

	private List<KeyValue> collectedFrom = new ArrayList<>();

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

	public void mergeFrom(final Relation r) {

		checkArgument(Objects.equals(getSource(), r.getSource()),"source ids must be equal");
		checkArgument(Objects.equals(getTarget(), r.getTarget()),"target ids must be equal");
		checkArgument(Objects.equals(getRelType(), r.getRelType()),"relType(s) must be equal");
		checkArgument(Objects.equals(getSubRelType(), r.getSubRelType()),"subRelType(s) must be equal");
		checkArgument(Objects.equals(getRelClass(), r.getRelClass()),"relClass(es) must be equal");

		setCollectedFrom(Stream.concat(getCollectedFrom().stream(), r.getCollectedFrom().stream())
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
				target.equals(relation.target) &&
				Objects.equals(collectedFrom, relation.collectedFrom);
	}

	@Override
	public int hashCode() {
		return Objects.hash(relType, subRelType, relClass, source, target, collectedFrom);
	}

}
