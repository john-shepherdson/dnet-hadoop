
package eu.dnetlib.dhp.broker.oa.util.aggregators.withRels;

import eu.dnetlib.dhp.schema.oaf.Dataset;

public class RelatedDataset {

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
