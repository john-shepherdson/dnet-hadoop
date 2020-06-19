
package eu.dnetlib.dhp.broker.oa.util.aggregators.withRels;

import java.io.Serializable;

import eu.dnetlib.broker.objects.Dataset;

public class RelatedDataset implements Serializable {

	/**
	 *
	 */
	private static final long serialVersionUID = 774487705184038324L;
	private String source;
	private String relType;
	private Dataset relDataset;

	public RelatedDataset() {
	}

	public RelatedDataset(final String source, final String relType, final Dataset relDataset) {
		this.source = source;
		this.relType = relType;
		this.relDataset = relDataset;
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

	public Dataset getRelDataset() {
		return relDataset;
	}

	public void setRelDataset(final Dataset relDataset) {
		this.relDataset = relDataset;
	}

}
