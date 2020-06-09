
package eu.dnetlib.dhp.schema.dump.oaf;

import eu.dnetlib.dhp.schema.common.ModelConstants;


import java.io.Serializable;

public class Publication extends Result implements Serializable {


	public Publication() {
		setType(ModelConstants.PUBLICATION_DEFAULT_RESULTTYPE.getClassname());
	}


}
