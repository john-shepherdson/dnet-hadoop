package eu.dnetlib.dhp.schema.common;

public class RelationInverse  {
    private String relation;
    private String inverse;
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

    public String getRelation() {
        return relation;
    }

    public RelationInverse setRelation(String relation) {
        this.relation = relation;
        return this;
    }

    public String getInverse() {
        return inverse;
    }

    public RelationInverse setInverse(String inverse) {
        this.inverse = inverse;
        return this;
    }


}
