
package eu.dnetlib.dhp.broker.oa.util.aggregators.withRels;

import java.io.Serializable;

import eu.dnetlib.broker.objects.Publication;

public class RelatedPublication implements Serializable {

	/**
	 *
	 */
	private static final long serialVersionUID = 9021609640411395128L;

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
