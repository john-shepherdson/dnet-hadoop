package eu.dnetlib.scholexplorer.relation;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.commons.io.IOUtils;

import java.io.Serializable;
import java.util.HashMap;

public class RelationMapper extends HashMap<String,RelInfo > implements Serializable {

    public static RelationMapper load() throws Exception {

        final String json = IOUtils.toString(RelationMapper.class.getResourceAsStream("relations.json"));

        ObjectMapper mapper = new ObjectMapper();
        return mapper.readValue(json, RelationMapper.class);
    }

}
