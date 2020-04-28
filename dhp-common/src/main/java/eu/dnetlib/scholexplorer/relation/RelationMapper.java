
package eu.dnetlib.scholexplorer.relation;

import java.io.Serializable;
import java.util.HashMap;

import org.apache.commons.io.IOUtils;

import com.fasterxml.jackson.databind.ObjectMapper;

public class RelationMapper extends HashMap<String, RelInfo> implements Serializable {

	public static RelationMapper load() throws Exception {

		final String json = IOUtils.toString(RelationMapper.class.getResourceAsStream("relations.json"));

		ObjectMapper mapper = new ObjectMapper();
		return mapper.readValue(json, RelationMapper.class);
	}
}
