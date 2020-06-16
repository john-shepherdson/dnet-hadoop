
package eu.dnetlib.dhp.broker.oa.util.aggregators.withRels;

import java.io.Serializable;

import eu.dnetlib.broker.objects.Dataset;

public class RelatedDataset implements Serializable {

	/**
	 *
	 */
	private static final long serialVersionUID = 774487705184038324L;
	private final String source;
	private final String relType;
	private final Dataset relDataset;

	public RelatedDataset(final String source, final String relType, final Dataset relDataset) {
		this.source = source;
		this.relType = relType;
		this.relDataset = relDataset;
	}

	public String getSource() {
		return source;
	}

	public String getRelType() {
		return relType;
	}

	public Dataset getRelDataset() {
		return relDataset;
	}

}
