
package eu.dnetlib.dhp.broker.oa.util.aggregators.withRels;

import java.io.Serializable;

import eu.dnetlib.broker.objects.OaBrokerRelatedPublication;

public class RelatedPublication implements Serializable {

	/**
	 *
	 */
	private static final long serialVersionUID = 9021609640411395128L;

	private String source;
	private String relType;
	private OaBrokerRelatedPublication relPublication;

	public RelatedPublication() {
	}

	public RelatedPublication(final String source, final String relType,
		final OaBrokerRelatedPublication relPublication) {
		this.source = source;
		this.relType = relType;
		this.relPublication = relPublication;
	}

	public String getSource() {
		return source;
	}

	public void setSource(final String source) {
		this.source = source;
	}

	public String getRelType() {
		return relType;
	}

	public void setRelType(final String relType) {
		this.relType = relType;
	}

	public OaBrokerRelatedPublication getRelPublication() {
		return relPublication;
	}

	public void setRelPublication(final OaBrokerRelatedPublication relPublication) {
		this.relPublication = relPublication;
	}

}
