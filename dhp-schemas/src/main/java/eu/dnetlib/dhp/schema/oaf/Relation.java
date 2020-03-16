package eu.dnetlib.dhp.schema.oaf;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.junit.Assert;

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
		Assert.assertEquals("source ids must be equal", getSource(), r.getSource());
		Assert.assertEquals("target ids must be equal", getTarget(), r.getTarget());
		Assert.assertEquals("relType(s) must be equal", getRelType(), r.getRelType());
		Assert.assertEquals("subRelType(s) must be equal", getSubRelType(), r.getSubRelType());
		Assert.assertEquals("relClass(es) must be equal", getRelClass(), r.getRelClass());
		setCollectedFrom(Stream.concat(getCollectedFrom().stream(), r.getCollectedFrom().stream())
				.distinct() // relies on KeyValue.equals
				.collect(Collectors.toList()));
	}

}
