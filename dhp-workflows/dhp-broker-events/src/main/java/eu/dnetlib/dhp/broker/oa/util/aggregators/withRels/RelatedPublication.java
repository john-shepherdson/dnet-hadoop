
package eu.dnetlib.dhp.broker.oa.util.aggregators.withRels;

import eu.dnetlib.dhp.schema.oaf.Publication;

public class RelatedPublication {

	private final String source;
	private final String relType;
	private final Publication relPublication;

	public RelatedPublication(final String source, final String relType, final Publication relPublication) {
		this.source = source;
		this.relType = relType;
		this.relPublication = relPublication;
	}

	public String getSource() {
		return source;
	}

	public String getRelType() {
		return relType;
	}

	public Publication getRelPublication() {
		return relPublication;
	}

}
