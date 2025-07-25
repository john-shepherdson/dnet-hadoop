package eu.dnetlib.dhp.actionmanager.affro;

import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.api.java.UDF1;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;
import scala.collection.mutable.WrappedArray;

import java.util.*;

import static org.apache.spark.sql.types.DataTypes.StringType;

public class AggregateAuthorUDF implements UDF1<WrappedArray<Row>, Row> {

    @Override
    public Row call(WrappedArray<Row> group) {
        List<Row> affiliations = new ArrayList<>();

        String id = null;
        String fullname = null;
        Boolean corresponding = null;
        WrappedArray<Row> contributor_roles = null;

        for (int i = 0; i < group.length(); i++) {
            Row entry = group.apply(i);
            if (id == null) id = entry.getAs("id");
            if (fullname == null) fullname = entry.getAs("fullname");
            if(corresponding == null) corresponding = entry.getAs("corresponding");
            if(contributor_roles == null) contributor_roles = entry.getAs("contributor_roles");
            String rawAffString = entry.getAs("raw_affiliation_string");
            WrappedArray<Row> matchArray = entry.getAs("Matchings");

            List<Row> matchList = new ArrayList<>();
            for (int j = 0; j < matchArray.length(); j++) {
                matchList.add(matchArray.apply(j));
            }

            List<Row> resolvedMatchings = regroupAndSelectDistinctMatch(matchList);
            Row affiliationRow = RowFactory.create(rawAffString, resolvedMatchings); // ← solo 2 campi!
            affiliations.add(affiliationRow);

        }

        return RowFactory.create(id, fullname, affiliations, corresponding, contributor_roles);
    }

    private List<Row> regroupAndSelectDistinctMatch(List<Row> inputGroups) {
        // Map: Value → [Confidence, Provenance, PID]
        Map<String, Tuple> valueMap = new HashMap<>();

        //for (List<Row> group : inputGroups) {
            for (Row row : inputGroups) {
                if (!"active".equalsIgnoreCase(row.getAs("Status"))) continue;

                String value = row.getAs("Value");
                double confidence = 0.0;
                if (row.getAs("Confidence") != null)
                    confidence = row.getAs("Confidence");
                String provenance = row.getAs("Provenance");
                String pid = row.getAs("PID");
                String country = row.getAs("Country");

                Tuple newValue = new Tuple(confidence, provenance, pid, country);
                // Update only if confidence is higher
                if (!valueMap.containsKey(value) )
                    valueMap.put(value, newValue);
                if(valueMap.get(value).confidence < confidence)
                    valueMap.replace(value, valueMap.get(value), newValue);
            }
        //}

        List<Row> result = new ArrayList<>();
        for (Map.Entry<String, Tuple> entry : valueMap.entrySet()) {
            Tuple t = entry.getValue();
            result.add(RowFactory.create(t.provenance, t.pid, entry.getKey(), t.confidence, "active", t.country));
        }

        return result;
    }

    // Helper class for holding multiple values
    private static class Tuple {
        double confidence;
        String provenance;
        String pid;
        String country;

        public Tuple(double confidence, String provenance, String pid, String country) {
            this.confidence = confidence;
            this.provenance = provenance;
            this.pid = pid;
            this.country = country;
        }
    }

}

