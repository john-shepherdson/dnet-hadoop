package eu.dnetlib.dhp.tag.udf;


import eu.dnetlib.dhp.tag.bean.Constraint;

import org.apache.spark.sql.api.java.UDF4;

import java.io.Serializable;
import java.util.Map;

/**
 * Spark UDF wrapper for AfterVerb.
 * Takes left JSON, right JSON/value, and a configuration map.
 */
public class AfterVerbUdf implements UDF4<String, String, Map<String, Object>, Constraint, Boolean>, Serializable {

    @Override
    public Boolean call(String leftJson, String rightJson, Map<String, Object> config, Constraint constraint) {
        return null;
    }
}

