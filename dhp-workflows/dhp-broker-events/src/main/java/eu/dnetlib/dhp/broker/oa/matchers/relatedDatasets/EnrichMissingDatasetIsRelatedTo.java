
package eu.dnetlib.dhp.broker.oa.matchers.relatedDatasets;

import eu.dnetlib.dhp.broker.model.Topic;
import eu.dnetlib.dhp.schema.common.ModelConstants;

public class EnrichMissingDatasetIsRelatedTo extends AbstractEnrichMissingDataset {

	public EnrichMissingDatasetIsRelatedTo() {
		super(Topic.ENRICH_MISSING_DATASET_IS_RELATED_TO);
	}

	@Override
	protected boolean filterByType(final String relType) {
		return relType.equals(ModelConstants.IS_RELATED_TO);
	}

}
