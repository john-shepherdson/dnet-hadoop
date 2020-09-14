
package eu.dnetlib.dhp.schema.dump.oaf.graph;

import java.io.Serializable;
import java.util.Objects;

import eu.dnetlib.dhp.schema.dump.oaf.Provenance;

/**
 * To represent the gereric relation between two entities. It has the following parameters:
 * - private Node source to represent the entity source of the relation
 * - private Node target to represent the entity target of the relation
 * - private RelType reltype to represent the semantics of the relation
 * - private Provenance provenance to represent the provenance of the relation
 */
public class Relation implements Serializable {
	private Node source;
	private Node target;
	private RelType reltype;
	private Provenance provenance;

	public Node getSource() {
		return source;
	}

	public void setSource(Node source) {
		this.source = source;
	}

	public Node getTarget() {
		return target;
	}

	public void setTarget(Node target) {
		this.target = target;
	}

	public RelType getReltype() {
		return reltype;
	}

	public void setReltype(RelType reltype) {
		this.reltype = reltype;
	}

	public Provenance getProvenance() {
		return provenance;
	}

	public void setProvenance(Provenance provenance) {
		this.provenance = provenance;
	}

	@Override
	public int hashCode() {

		return Objects.hash(source.getId(), target.getId(), reltype.getType() + ":" + reltype.getName());
	}

	public static Relation newInstance(Node source, Node target, RelType reltype, Provenance provenance) {
		Relation relation = new Relation();
		relation.source = source;
		relation.target = target;
		relation.reltype = reltype;
		relation.provenance = provenance;
		return relation;
	}
}
