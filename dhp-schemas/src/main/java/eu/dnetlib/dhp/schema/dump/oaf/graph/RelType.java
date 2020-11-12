
package eu.dnetlib.dhp.schema.dump.oaf.graph;

import java.io.Serializable;

/**
 * To represent the semantics of the generic relation between two entities. It has the following parameters: - private
 * String name to store the semantics of the relation (i.e. isAuthorInstitutionOf). It corresponds to the relclass
 * parameter in the relation represented in the internal model represented in the internal model - private String type
 * to store the type of the relation (i.e. affiliation). It corresponds to the subreltype parameter of the relation
 * represented in theinternal model
 */
public class RelType implements Serializable {
	private String name; // relclass
	private String type; // subreltype

	public String getName() {
		return name;
	}

	public void setName(String name) {
		this.name = name;
	}

	public String getType() {
		return type;
	}

	public void setType(String type) {
		this.type = type;
	}

	public static RelType newInstance(String name, String type) {
		RelType rel = new RelType();
		rel.name = name;
		rel.type = type;
		return rel;
	}
}
