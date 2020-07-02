
package eu.dnetlib.dhp.oa.provision.model;

import java.util.List;

import com.google.common.collect.Lists;

import eu.dnetlib.dhp.oa.provision.RelationList;
import eu.dnetlib.dhp.oa.provision.SortableRelation;
import eu.dnetlib.dhp.schema.common.ModelSupport;

public class ProvisionModelSupport {

	public static Class[] getModelClasses() {
		List<Class<?>> modelClasses = Lists.newArrayList(ModelSupport.getOafModelClasses());
		modelClasses
			.addAll(
				Lists
					.newArrayList(
						RelatedEntityWrapper.class,
						JoinedEntity.class,
						RelatedEntity.class,
						SortableRelationKey.class,
						SortableRelation.class,
						RelationList.class));
		return modelClasses.toArray(new Class[] {});
	}
}
