
package eu.dnetlib.dhp.bulktag.community;

import java.io.Serializable;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Type;
import java.util.Collection;
import java.util.List;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;

import eu.dnetlib.dhp.bulktag.criteria.VerbResolver;

/** Created by miriam on 02/08/2018. */
public class Constraints implements Serializable {
	private static final Log log = LogFactory.getLog(Constraints.class);

	private List<Constraint> constraint;

	public List<Constraint> getConstraint() {
		return constraint;
	}

	public void setConstraint(List<Constraint> constraint) {
		this.constraint = constraint;
	}

	public void setSc(String json) {
		Type collectionType = new TypeToken<Collection<Constraint>>() {
		}.getType();
		constraint = new Gson().fromJson(json, collectionType);
	}

	void setSelection(VerbResolver resolver) {
		for (Constraint st : constraint) {

			try {
				st.setSelection(resolver);
			} catch (NoSuchMethodException | IllegalAccessException | InvocationTargetException
				| InstantiationException e) {
				log.error(e.getMessage());
			}
		}
	}

	// Constraint in and
	public boolean verifyCriteria(final Map<String, List<String>> param) {

		for (Constraint sc : constraint) {
			boolean verified = false;
			if(!param.containsKey(sc.getField()))
				return false;
			for (String value : param.get(sc.getField())) {
				if (sc.verifyCriteria(value.trim())) {
					verified = true;
				}
			}
			if (!verified)
				return verified;
		}
		return true;
	}
}
