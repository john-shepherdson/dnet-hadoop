
package eu.dnetlib.dhp.broker.oa.util.aggregators.withRels;

import java.io.Serializable;

import eu.dnetlib.broker.objects.Software;

public class RelatedSoftware implements Serializable {

	/**
	 *
	 */
	private static final long serialVersionUID = 7573383356943300157L;
	private final String source;
	private final String relType;
	private final Software relSoftware;

	public RelatedSoftware(final String source, final String relType, final Software relSoftware) {
		this.source = source;
		this.relType = relType;
		this.relSoftware = relSoftware;
	}

	public String getSource() {
		return source;
	}

	public String getRelType() {
		return relType;
	}

	public Software getRelSoftware() {
		return relSoftware;
	}

}
