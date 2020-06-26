
package eu.dnetlib.dhp.broker.oa.util.aggregators.withRels;

import java.io.Serializable;

import eu.dnetlib.broker.objects.OaBrokerRelatedDataset;

public class RelatedDataset implements Serializable {

	/**
	 *
	 */
	private static final long serialVersionUID = 774487705184038324L;

	private String source;
	private OaBrokerRelatedDataset relDataset;

	public RelatedDataset() {
	}

	public RelatedDataset(final String source, final OaBrokerRelatedDataset relDataset) {
		this.source = source;
		this.relDataset = relDataset;
	}

	public String getSource() {
		return source;
	}

	public void setSource(final String source) {
		this.source = source;
	}

	public OaBrokerRelatedDataset getRelDataset() {
		return relDataset;
	}

	public void setRelDataset(final OaBrokerRelatedDataset relDataset) {
		this.relDataset = relDataset;
	}

}
