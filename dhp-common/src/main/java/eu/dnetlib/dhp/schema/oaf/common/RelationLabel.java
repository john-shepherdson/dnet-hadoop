
package eu.dnetlib.dhp.schema.oaf.common;

import eu.dnetlib.dhp.schema.oaf.Relation;

public class RelationLabel {
	private final Relation.RELCLASS relClass;
	private final Relation.RELTYPE relType;
	private final Relation.SUBRELTYPE subReltype;

	public RelationLabel(Relation.RELCLASS relClass, Relation.RELTYPE relType, Relation.SUBRELTYPE subReltype) {
		this.relClass = relClass;
		this.relType = relType;
		this.subReltype = subReltype;

	}

	public RelationLabel inverse() {
		return  new RelationLabel(relClass.getInverse(), relType, subReltype);
	}

	public Relation.RELTYPE getRelType() {
		return relType;
	}

	public Relation.SUBRELTYPE getSubReltype() {
		return subReltype;
	}

	public Relation.RELCLASS getRelClass() {
		return relClass;
	}
}
