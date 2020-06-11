
package eu.dnetlib.dhp.broker.oa.util.aggregators.withRels;

import eu.dnetlib.dhp.schema.oaf.Dataset;
import eu.dnetlib.dhp.schema.oaf.Project;
import eu.dnetlib.dhp.schema.oaf.Publication;
import eu.dnetlib.dhp.schema.oaf.Software;

public class RelatedEntityFactory {

	@SuppressWarnings("unchecked")
	public static <RT, T> RT newRelatedEntity(final String sourceId, final String relType, final T target,
		final Class<RT> clazz) {
		if (clazz == RelatedProject.class) {
			return (RT) new RelatedProject(sourceId, relType, (Project) target);
		}
		if (clazz == RelatedSoftware.class) {
			return (RT) new RelatedSoftware(sourceId, relType, (Software) target);
		}
		if (clazz == RelatedDataset.class) {
			return (RT) new RelatedDataset(sourceId, relType, (Dataset) target);
		}
		if (clazz == RelatedPublication.class) {
			return (RT) new RelatedPublication(sourceId, relType, (Publication) target);
		}
		return null;
	}
}
