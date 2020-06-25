
package eu.dnetlib.dhp.broker.oa.util;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

import eu.dnetlib.dhp.broker.model.Event;
import eu.dnetlib.dhp.broker.oa.util.aggregators.simple.ResultGroup;
import eu.dnetlib.dhp.schema.common.ModelSupport;

public class BrokerConstants {

	public static final String OPEN_ACCESS = "OPEN";
	public static final String IS_MERGED_IN_CLASS = "isMergedIn";

	public static final float MIN_TRUST = 0.25f;
	public static final float MAX_TRUST = 1.00f;

	public static final int MAX_NUMBER_OF_RELS = 20;

	public static Class<?>[] getModelClasses() {
		final Set<Class<?>> list = new HashSet<>();
		list.addAll(Arrays.asList(ModelSupport.getOafModelClasses()));
		list.addAll(Arrays.asList(ResultGroup.class, Event.class));
		return list.toArray(new Class[] {});
	}

}
