
package eu.dnetlib.dhp.schema.oaf.common;

public class RelationInverse {
	private String relClass;
	private String inverseRelClass;
	private String relType;
	private String subReltype;

	public String getRelType() {
		return relType;
	}

	public RelationInverse setRelType(String relType) {
		this.relType = relType;
		return this;
	}

	public String getSubReltype() {
		return subReltype;
	}

	public RelationInverse setSubReltype(String subReltype) {
		this.subReltype = subReltype;
		return this;
	}

	public String getRelClass() {
		return relClass;
	}

	public RelationInverse setRelClass(String relClass) {
		this.relClass = relClass;
		return this;
	}

	public String getInverseRelClass() {
		return inverseRelClass;
	}

	public RelationInverse setInverseRelClass(String inverseRelClass) {
		this.inverseRelClass = inverseRelClass;
		return this;
	}

}
