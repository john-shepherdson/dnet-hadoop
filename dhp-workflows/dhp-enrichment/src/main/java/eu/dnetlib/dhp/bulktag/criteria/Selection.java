
package eu.dnetlib.dhp.bulktag.criteria;
import java.io.IOException;
import java.io.Serializable;
import java.lang.reflect.InvocationTargetException;

public interface Selection extends Serializable {

	boolean apply(Object value) throws IOException, InvocationTargetException, NoSuchMethodException, IllegalAccessException, InstantiationException;

	boolean apply(Object value, Object otherEntityValue) throws IOException;

}
