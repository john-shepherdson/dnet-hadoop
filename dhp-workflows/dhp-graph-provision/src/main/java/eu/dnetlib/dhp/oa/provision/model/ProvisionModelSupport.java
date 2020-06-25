
package eu.dnetlib.dhp.oa.provision.model;

import java.util.List;

import com.google.common.collect.Lists;

import eu.dnetlib.dhp.schema.common.ModelSupport;

public class ProvisionModelSupport {

	public static Class[] getModelClasses() {
		List<Class<?>> modelClasses = Lists.newArrayList(ModelSupport.getOafModelClasses());
		modelClasses
			.addAll(
				Lists
					.newArrayList(
						TypedRow.class,
						RelatedEntityWrapper.class,
						JoinedEntity.class,
						RelatedEntity.class,
						SortableRelationKey.class));
		return modelClasses.toArray(new Class[] {});
	}
}
