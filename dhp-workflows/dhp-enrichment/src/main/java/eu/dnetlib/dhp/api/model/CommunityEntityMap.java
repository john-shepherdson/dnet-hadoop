
package eu.dnetlib.dhp.api.model;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

public class CommunityEntityMap extends HashMap<String, List<String>> {

	public CommunityEntityMap() {
		super();
	}

	public List<String> get(String key) {

		if (super.get(key) == null) {
			return new ArrayList<>();
		}
		return super.get(key);
	}

	public void add(String key, String value){
		if(!super.containsKey(key)){
			super.put(key, new ArrayList<>());
		}
		super.get(key).add(value);
	}


}
