package eu.dnetlib.dhp.graph;

import eu.dnetlib.dhp.schema.oaf.OafEntity;
import eu.dnetlib.dhp.schema.oaf.Relation;
import scala.Tuple2;

import java.io.Serializable;
import java.util.List;

public class JoinedEntity implements Serializable {

    private String type;

    private OafEntity entity;

    private List<Tuple2<Relation, RelatedEntity>> links;

    public String getType() {
        return type;
    }

    public JoinedEntity setType(String type) {
        this.type = type;
        return this;
    }

    public OafEntity getEntity() {
        return entity;
    }

    public JoinedEntity setEntity(OafEntity entity) {
        this.entity = entity;
        return this;
    }

    public List<Tuple2<Relation, RelatedEntity>> getLinks() {
        return links;
    }

    public JoinedEntity setLinks(List<Tuple2<Relation, RelatedEntity>> links) {
        this.links = links;
        return this;
    }
}
