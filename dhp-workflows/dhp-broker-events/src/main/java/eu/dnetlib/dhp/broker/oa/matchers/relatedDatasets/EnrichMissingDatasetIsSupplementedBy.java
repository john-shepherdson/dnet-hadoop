
package eu.dnetlib.dhp.broker.oa.matchers.relatedDatasets;

import eu.dnetlib.dhp.broker.model.Topic;
import eu.dnetlib.dhp.schema.common.ModelConstants;

public class EnrichMissingDatasetIsSupplementedBy extends AbstractEnrichMissingDataset {

	public EnrichMissingDatasetIsSupplementedBy() {
		super(Topic.ENRICH_MISSING_DATASET_IS_SUPPLEMENTED_BY);
	}

	@Override
	protected boolean filterByType(final String relType) {
		return relType.equals(ModelConstants.IS_SUPPLEMENTED_BY);
	}

}
