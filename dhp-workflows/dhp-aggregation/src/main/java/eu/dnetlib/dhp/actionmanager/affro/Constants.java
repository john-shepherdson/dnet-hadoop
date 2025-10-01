package eu.dnetlib.dhp.actionmanager.affro;


import static org.apache.spark.sql.types.DataTypes.StringType;
import static org.apache.spark.sql.types.DataTypes.IntegerType;
import static org.apache.spark.sql.types.DataTypes.BooleanType;
import static org.apache.spark.sql.types.DataTypes.DoubleType;

import java.io.Serializable;

import org.apache.spark.sql.types.*;

public class Constants implements Serializable {


    public final static StructType PID_STRUCT = new StructType()
            .add("schema", StringType)
            .add("value", StringType);


    public final static ArrayType PID_SCHEMA = DataTypes.createArrayType(PID_STRUCT);

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
            .add("raw_affiliation_string", StringType)
            .add("corresponding", DataTypes.BooleanType)
            .add("contributor_roles", DataTypes.createArrayType(new StructType()
                    .add("schema", StringType)
                    .add("name", StringType)
                    .add("value", StringType)))
            .add("pids", PID_SCHEMA)
            ;



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
                                                            .add("qualifier", new StructType()
                                                                    .add("classid", StringType)
                                                            )
                                            ))
                                            .add("rawAffiliationString", DataTypes.createArrayType(StringType))));

    public final static StructType AUTHOR_SCHEMA = new StructType()
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
            .add("pids", PID_SCHEMA);

    public final static String IIS_QUERY = "SELECT id, authors, affiliations FROM mh.extracted_document_metadata_prod";

    public final static StructType PUBLISHER_SCHEMA = new StructType()
            .add("id", StringType)
            .add("doi", StringType)
            .add("parsing_output", new StructType()
                            .add("doi", StringType)
                            .add("authors", DataTypes
                                .createArrayType(AUTHOR_SCHEMA))
                            .add("parser", StringType)

            )
            .add("success", DataTypes.BooleanType)
            .add("reason_of_failure", StringType)
           ;

    public final static StructType MATCHING_SCHEMA = new StructType()
            .add("provenance", StringType)
            .add("pid", StringType)
            .add("value", StringType)
            .add("confidence", DataTypes.DoubleType)
            .add("status", StringType)
            .add("country", StringType)
            .add("name", StringType)
            ;


    public final static ArrayType MATCHING_ARRAY_SCHEMA =  DataTypes.createArrayType(MATCHING_SCHEMA);

    public final static StructType AFFILIATION_SCHEMA = new StructType()
            .add("affiliation", StringType)
            .add("matchings", MATCHING_ARRAY_SCHEMA)
            ;


    public final static StructType AFFILIATION_STRING_SCHEMA = new StructType()
            .add("raw_affiliation_string", StringType)

            ;


    public final static StructType AUTHOR_AGGREGATED_SCHEMA = new StructType()
            .add("id", StringType)
            .add("fullname", StringType)
            .add("affiliations", DataTypes.createArrayType(
                    new StructType()
                            .add("raw_affiliation_string", StringType)
                            .add("matchings", MATCHING_ARRAY_SCHEMA)
            ))
            .add("corresponding", DataTypes.BooleanType)
            .add("contributor_roles", DataTypes.createArrayType(new StructType()
                    .add("schema", StringType)
                    .add("name", StringType)
                    .add("value", StringType)))
            .add("pids", PID_SCHEMA);


    public final static StructType RESULT_MATCHED_SCHEMA = new StructType()
            .add("id", StringType)
            .add("authors", DataTypes.createArrayType(
                    new StructType()
                            .add("fullname", StringType)
                            .add("affiliations", DataTypes.createArrayType(
                                    new StructType()
                                            .add("raw_affiliation_string", StringType)
                                            .add("Matchings", MATCHING_ARRAY_SCHEMA)
                            ))
                            .add("corresponding", DataTypes.BooleanType)
                            .add("contributor_roles", DataTypes.createArrayType(new StructType()
                                    .add("schema", StringType)
                                    .add("name", StringType)
                                    .add("value", StringType)))
                            .add("pids", PID_SCHEMA)
                    ))
            .add("organizations",MATCHING_ARRAY_SCHEMA);

    public final static StructType DATACITE_CREATOR_SCHEMA = new StructType()
            .add("name", StringType, true)
            .add("givenName", StringType, true)
            .add("familyName", StringType, true)
            .add("nameType", StringType, true)
            .add("affiliation", DataTypes.createArrayType(StringType), true)
            .add("nameIdentifiers", DataTypes.createArrayType(StringType), true)
            ;

    public final static StructType DATACITE_ATTRIBUTES_SCHEMA = new StructType()
            .add("doi", StringType)
            .add("creators", DATACITE_CREATOR_SCHEMA)
            ;
    public final static StructType DATACITE_INPUT_SCHEMA = new StructType()
            .add("doi", StringType)
            .add("attributes", DATACITE_ATTRIBUTES_SCHEMA);


    public static StructType CROSSREF_AUTHOR_SCHEMA = new StructType()
            .add("given", StringType)
            .add("family", StringType)
            .add("affiliation", DataTypes.createArrayType(StringType));

    public static final StructType CROSSREF_INPUT_SCHEMA = new StructType()
            .add("DOI", StringType)
            .add("author", DataTypes.createArrayType(CROSSREF_AUTHOR_SCHEMA));

    public static final StructType OPENAPC_INPUT_SCHEMA = new StructType()
            .add("doi", StringType)
            .add("matchings", MATCHING_ARRAY_SCHEMA);



}
