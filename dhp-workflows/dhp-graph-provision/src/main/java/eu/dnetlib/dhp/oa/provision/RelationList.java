
package eu.dnetlib.dhp.oa.provision;

import java.io.Serializable;
import java.util.PriorityQueue;
import java.util.Queue;

import eu.dnetlib.dhp.schema.oaf.Relation;

public class RelationList implements Serializable {

	private Queue<Relation> relations;

	public RelationList() {
		this.relations = new PriorityQueue<>(new RelationComparator());
	}

	public Queue<Relation> getRelations() {
		return relations;
	}

	public void setRelations(Queue<Relation> relations) {
		this.relations = relations;
	}
}
