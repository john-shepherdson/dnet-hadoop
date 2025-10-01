package eu.dnetlib.dhp.actionmanager.bipaffiliations;

import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;

import static org.apache.spark.sql.types.DataTypes.StringType;

public class Constants {

    public final static StructType MATCHING_SCHEMA = new StructType()
            .add("provenance", StringType)
            .add("pid", StringType)
            .add("value", StringType)
            .add("confidence", DataTypes.DoubleType)
            .add("status", StringType)
            .add("country", StringType)
            .add("name", StringType)
            ;


    public final static StructType RESULT_MATCHED_SCHEMA = new StructType()
            .add("id", StringType)
            .add("authors", DataTypes.createArrayType(
                    new StructType()
                            .add("fullname", StringType)
                            .add("affiliations", DataTypes.createArrayType(
                                    new StructType()
                                            .add("raw_affiliation_string", StringType)
                                            .add("Matchings", DataTypes.createArrayType(MATCHING_SCHEMA))
                            ))
                            .add("corresponding", DataTypes.BooleanType)
                            .add("contributor_roles", DataTypes.createArrayType(new StructType()
                                    .add("schema", StringType)
                                    .add("name", StringType)
                                    .add("value", StringType)))
            ))
            .add("organizations",DataTypes.createArrayType(MATCHING_SCHEMA));

    public static final StructType OPENAPC_INPUT_SCHEMA = new StructType()
            .add("doi", StringType)
            .add("matchings", DataTypes.createArrayType(MATCHING_SCHEMA));
}
