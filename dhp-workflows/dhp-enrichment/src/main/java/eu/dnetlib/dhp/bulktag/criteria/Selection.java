
package eu.dnetlib.dhp.bulktag.criteria;

import eu.dnetlib.dhp.bulktag.community.ProtoMap;

import java.io.IOException;
import java.io.Serializable;

public interface Selection extends Serializable {

	boolean apply(Object value) throws IOException;

	boolean apply(Object value, Object otherEntityValue) throws IOException;

	//boolean apply(Object value, Object otherEntityValue, ProtoMap paramMap) throws IOException;
}
