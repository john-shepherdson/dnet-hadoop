
package eu.dnetlib.dhp.resulttocommunityfromorganization;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

public class OrganizationMap extends HashMap<String, List<String>> {

	public OrganizationMap() {
		super();
	}

	public List<String> get(String key) {

		if (super.get(key) == null) {
			return new ArrayList<>();
		}
		return super.get(key);
	}
}
