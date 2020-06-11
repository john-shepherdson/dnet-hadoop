
package eu.dnetlib.dhp.broker.oa.util.aggregators.withRels;

import eu.dnetlib.dhp.schema.oaf.Software;

public class RelatedSoftware {

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
