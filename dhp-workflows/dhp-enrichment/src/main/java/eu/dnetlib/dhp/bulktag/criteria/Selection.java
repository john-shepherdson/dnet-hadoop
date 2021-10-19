
package eu.dnetlib.dhp.bulktag.criteria;

import java.io.Serializable;

public interface Selection extends Serializable {

	boolean apply(String value);
}
