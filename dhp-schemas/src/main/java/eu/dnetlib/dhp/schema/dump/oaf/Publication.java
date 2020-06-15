
package eu.dnetlib.dhp.schema.dump.oaf;

import java.io.Serializable;

import eu.dnetlib.dhp.schema.common.ModelConstants;

public class Publication extends Result implements Serializable {

	public Publication() {
		setType(ModelConstants.PUBLICATION_DEFAULT_RESULTTYPE.getClassname());
	}

}
