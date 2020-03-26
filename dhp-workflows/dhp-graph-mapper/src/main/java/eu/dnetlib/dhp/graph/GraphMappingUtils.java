package eu.dnetlib.dhp.graph;

import java.util.Map;

import com.google.common.collect.Maps;

import eu.dnetlib.dhp.schema.oaf.Dataset;
import eu.dnetlib.dhp.schema.oaf.Datasource;
import eu.dnetlib.dhp.schema.oaf.Organization;
import eu.dnetlib.dhp.schema.oaf.OtherResearchProduct;
import eu.dnetlib.dhp.schema.oaf.Project;
import eu.dnetlib.dhp.schema.oaf.Publication;
import eu.dnetlib.dhp.schema.oaf.Relation;
import eu.dnetlib.dhp.schema.oaf.Software;

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
