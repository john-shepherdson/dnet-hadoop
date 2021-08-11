
package eu.dnetlib.dhp.broker.oa.util;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

import eu.dnetlib.dhp.broker.model.Event;
import eu.dnetlib.dhp.broker.oa.util.aggregators.simple.ResultGroup;
import eu.dnetlib.dhp.schema.common.ModelConstants;
import eu.dnetlib.dhp.schema.common.ModelSupport;

public class BrokerConstants {

	private BrokerConstants() {
	}

	public static final String OPEN_ACCESS = "OPEN";
	public static final String IS_MERGED_IN_CLASS = ModelConstants.IS_MERGED_IN;

	public static final String COLLECTED_FROM_REL = "collectedFrom";

	public static final String HOSTED_BY_REL = "hostedBy";

	public static final float MIN_TRUST = 0.25f;
	public static final float MAX_TRUST = 1.00f;

	public static final int MAX_NUMBER_OF_RELS = 20;

	public static final int MAX_STRING_SIZE = 3000;

	public static final int MAX_LIST_SIZE = 50;

	public static Class<?>[] getModelClasses() {
		final Set<Class<?>> list = new HashSet<>();
		list.addAll(Arrays.asList(ModelSupport.getOafModelClasses()));
		list.addAll(Arrays.asList(ResultGroup.class, Event.class));
		return list.toArray(new Class[] {});
	}

}
