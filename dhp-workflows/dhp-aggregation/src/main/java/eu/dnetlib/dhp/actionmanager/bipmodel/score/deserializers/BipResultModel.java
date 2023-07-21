
package eu.dnetlib.dhp.actionmanager.bipmodel.score.deserializers;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import eu.dnetlib.dhp.actionmanager.bipmodel.Score;

/**
 * Class that maps the model of the bipFinder! input data.
 * Only needed for deserialization purposes
 */

public class BipResultModel extends HashMap<String, List<Score>> implements Serializable {

	public BipResultModel() {
		super();
	}

	public List<Score> get(String key) {

		if (super.get(key) == null) {
			return new ArrayList<>();
		}
		return super.get(key);
	}

}
