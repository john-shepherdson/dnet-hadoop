
package eu.dnetlib.dhp.actionmanager.createunresolvedentities.model;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

/**
 * Class that maps the model of the bipFinder! input data.
 * Only needed for deserialization purposes
 */

public class BipDeserialize extends HashMap<String, List<Score>> implements Serializable {

	public BipDeserialize() {
		super();
	}

	public List<Score> get(String key) {

		if (super.get(key) == null) {
			return new ArrayList<>();
		}
		return super.get(key);
	}

}
