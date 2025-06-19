
package eu.dnetlib.dhp.actionmanager.bipaffiliations;

import static org.apache.spark.sql.types.DataTypes.StringType;

import java.io.Serializable;

import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;

public class Constants implements Serializable {
	public final static StructType OALEX_SCHEMA = new StructType()
		.add("id", DataTypes.StringType)
		.add("doi", DataTypes.StringType)
		.add(
			"authorships", DataTypes
				.createArrayType(
					new StructType()
						.add("author", new StructType().add("display_name", StringType))
						.add("raw_affiliation_strings", DataTypes.createArrayType(DataTypes.StringType))));

	private final static StructType GRAPH_SCHEMA = new StructType()
		.add("id", DataTypes.StringType) // oaire id created on the fly if result has doi as identifier
		.add(
			"authors", DataTypes
				.createArrayType(
					new StructType()
						.add("fullname", DataTypes.StringType)
						.add("raw_affiliation_strings", DataTypes.createArrayType(DataTypes.StringType))));

}
