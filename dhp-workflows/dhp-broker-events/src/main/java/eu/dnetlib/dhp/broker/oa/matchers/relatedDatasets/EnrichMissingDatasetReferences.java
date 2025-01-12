
package eu.dnetlib.dhp.broker.oa.matchers.relatedDatasets;

import eu.dnetlib.dhp.broker.model.Topic;
import eu.dnetlib.dhp.schema.common.ModelConstants;

public class EnrichMissingDatasetReferences extends AbstractEnrichMissingDataset {

	public EnrichMissingDatasetReferences() {
		super(Topic.ENRICH_MISSING_DATASET_REFERENCES);
	}

	@Override
	protected boolean filterByType(final String relType) {
		return relType.equals(ModelConstants.REFERENCES);
	}

}
