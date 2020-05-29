
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
						EntityRelEntity.class,
						JoinedEntity.class,
						RelatedEntity.class,
						Tuple2.class,
						SortableRelation.class));
		return modelClasses.toArray(new Class[] {});
	}
}
