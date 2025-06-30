package eu.dnetlib.dhp.actionmanager.affro;


import static org.apache.spark.sql.types.DataTypes.StringType;
import static org.apache.spark.sql.types.DataTypes.IntegerType;

import java.io.Serializable;

import org.apache.spark.sql.types.*;

public class Constants implements Serializable {


    public final static StructType IIS_SCHEMA = new StructType()
            .add("id", StringType)
            .add("authors", DataTypes.createArrayType( new StructType()
                    .add("authorfullname", StringType)
                    .add("affiliationpositions", DataTypes.createArrayType(IntegerType))
            ))
            .add("affiliations", DataTypes.createArrayType(new StructType()
                    .add("rawtext", StringType)
            ))
            ;
    public final static StructType DATASET_SCHEMA = new StructType()
            .add("id", StringType)
            .add("fullname", StringType)
            .add("raw_affiliation_string", StringType);

    public final static StructType OALEX_SCHEMA = new StructType()
            .add("id", StringType)
            .add("doi", StringType)
            .add(
                    "authorships", DataTypes
                            .createArrayType(
                                    new StructType()
                                            .add("author", new StructType().add("display_name", StringType))
                                            .add("raw_affiliation_strings", DataTypes.createArrayType(StringType))));

    public final static StructType GRAPH_SCHEMA = new StructType()
            .add("id", StringType) // oaire id created on the fly if result has doi as identifier
            .add(
                    "author", DataTypes
                            .createArrayType(
                                    new StructType()
                                            .add("fullname", StringType)
                                            .add("pid", DataTypes.createArrayType(
                                                    new StructType()
                                                            .add("value", StringType)
                                                            .add("typeCode", StringType)
                                                            .add("typeLabel", StringType)
                                            ))
                                            .add("rawAffiliationString", DataTypes.createArrayType(StringType))));

    public final static String IIS_QUERY = "SELECT id, authors, affiliations FROM mh.extracted_document_metadata_prod";

    public final static StructType PUBLISHER_SCHEMA = new StructType()
            .add("id", StringType)
            .add("doi", StringType)
            .add("parsing_output", new StructType()
                            .add("doi", StringType)
                            .add("authors", DataTypes
                                .createArrayType(
                                    new StructType()
                                            .add("name", new StructType()
                                                    .add("full", StringType)
                                                    .add("first", StringType)
                                                    .add("last", StringType)
                                                    .add("type", StringType))
                                            .add("corresponding", DataTypes.BooleanType)
                                            .add("contributor_roles", DataTypes.createArrayType(new StructType()
                                                    .add("schema", StringType)
                                                    .add("name", StringType)
                                                    .add("value", StringType)))
                                            .add("raw_affiliations", DataTypes.createArrayType(StringType))
                                            .add("pids", DataTypes
                                                    .createArrayType(new StructType()
                                                            .add("schema", StringType)
                                                            .add("value", StringType)))
                                )
                            )
                            .add("parser", StringType)

            )
            .add("success", DataTypes.BooleanType)
            .add("reason_of_failure", StringType)
           ;

    public final static StructType MATCHING_SCHEMA = new StructType()
            .add("Provenance", StringType)
            .add("PID", StringType)
            .add("Value", StringType)
            .add("Confidence", DataTypes.DoubleType)
            .add("Status", StringType)
            ;


    public final static ArrayType MATCHING_ARRAY_SCHEMA =  DataTypes.createArrayType(MATCHING_SCHEMA);

    public final static StructType AFFILIATION_SCHEMA = new StructType()
            .add("Affiliation", StringType)
            .add("Matchings", MATCHING_ARRAY_SCHEMA)
            ;

    public final static StructType AFFILIATION_STRING_SCHEMA = new StructType()
            .add("raw_affiliation_string", StringType)

            ;


}
