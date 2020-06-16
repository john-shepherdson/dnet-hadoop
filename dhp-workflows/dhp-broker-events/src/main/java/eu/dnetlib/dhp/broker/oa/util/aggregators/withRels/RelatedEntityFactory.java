
package eu.dnetlib.dhp.broker.oa.util.aggregators.withRels;

import eu.dnetlib.dhp.broker.oa.util.ConversionUtils;
import eu.dnetlib.dhp.schema.oaf.Dataset;
import eu.dnetlib.dhp.schema.oaf.Project;
import eu.dnetlib.dhp.schema.oaf.Publication;
import eu.dnetlib.dhp.schema.oaf.Software;

public class RelatedEntityFactory {

	@SuppressWarnings("unchecked")
	public static <RT, T> RT newRelatedEntity(final String sourceId,
		final String relType,
		final T target,
		final Class<RT> clazz) {

		if (clazz == RelatedProject.class) {
			return (RT) new RelatedProject(sourceId, relType, ConversionUtils.oafProjectToBrokerProject((Project) target));
		} else if (clazz == RelatedSoftware.class) {
			return (RT) new RelatedSoftware(sourceId, relType, ConversionUtils.oafSoftwareToBrokerSoftware((Software) target));
		} else if (clazz == RelatedDataset.class) {
			return (RT) new RelatedDataset(sourceId, relType, ConversionUtils.oafDatasetToBrokerDataset((Dataset) target));
		} else if (clazz == RelatedPublication.class) {
			return (RT) new RelatedPublication(sourceId, relType, ConversionUtils.oafPublicationToBrokerPublication((Publication) target));
		} else {
			return null;
		}
	}
}
