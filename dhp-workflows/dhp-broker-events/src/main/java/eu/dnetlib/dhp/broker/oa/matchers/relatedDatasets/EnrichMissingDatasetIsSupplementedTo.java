
package eu.dnetlib.dhp.broker.oa.matchers.relatedDatasets;

import eu.dnetlib.dhp.broker.model.Topic;

public class EnrichMissingDatasetIsSupplementedTo extends AbstractEnrichMissingDataset {

	public EnrichMissingDatasetIsSupplementedTo() {
		super(Topic.ENRICH_MISSING_DATASET_IS_SUPPLEMENTED_TO);
	}

	@Override
	protected boolean filterByType(final String relType) {
		return relType.equals("isSupplementedTo");
	}

}
