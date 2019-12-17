package eu.dnetlib.dhp.graph;

import com.google.common.collect.Maps;
import eu.dnetlib.dhp.schema.oaf.*;

import java.util.Map;

public class GraphMappingUtils {

    public final static Map<String, Class> types = Maps.newHashMap();

    static {
        types.put("datasource", Datasource.class);
        types.put("organization", Organization.class);
        types.put("project", Project.class);
        types.put("dataset", Dataset.class);
        types.put("otherresearchproduct", OtherResearchProduct.class);
        types.put("software", Software.class);
        types.put("publication", Publication.class);
        types.put("relation", Relation.class);
    }

}
