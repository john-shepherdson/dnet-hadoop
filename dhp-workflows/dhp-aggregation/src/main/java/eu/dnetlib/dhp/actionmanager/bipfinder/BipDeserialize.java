
package eu.dnetlib.dhp.actionmanager.bipfinder;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

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
