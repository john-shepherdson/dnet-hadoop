package eu.dnetlib.dhp.actionmanager.affro;

import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.api.java.UDF1;
import scala.collection.mutable.WrappedArray;

import java.util.*;

public class AggregateAuthorUDF implements UDF1<WrappedArray<Row>, Row> {

    @Override
    public Row call(WrappedArray<Row> group) {
        List<String> affiliations = new ArrayList<>();
        List<List<Row>> allMatchings = new ArrayList<>();
        String id = null;
        String fullname = null;
        String rawAffiliation = null;

        for (int i = 0; i < group.length(); i++) {
            Row entry = group.apply(i);

            affiliations.add(entry.getAs("raw_affiliation_string"));

            WrappedArray<Row> matchArray = entry.getAs("Matchings");
            List<Row> matchList = new ArrayList<>();
            for (int j = 0; j < matchArray.length(); j++) {
                matchList.add(matchArray.apply(j));
            }
            allMatchings.add(matchList);

            // Prendiamo id e fullname dalla prima riga
            if (id == null) id = entry.getAs("id");
            if (fullname == null) fullname = entry.getAs("fullname");
            if (rawAffiliation == null) rawAffiliation = entry.getAs("raw_affiliation_string");
        }

        List<Row> regrouped = regroupAndSelectDistinctMatch(allMatchings);

        return RowFactory.create(id, fullname, rawAffiliation, regrouped);
    }

    private List<Row> regroupAndSelectDistinctMatch(List<List<Row>> inputGroups) {
        // Map: Value → [Confidence, Provenance, PID]
        Map<String, Tuple> valueMap = new HashMap<>();

        for (List<Row> group : inputGroups) {
            for (Row row : group) {
                if (!"active".equalsIgnoreCase(row.getAs("Status"))) continue;

                String value = row.getAs("Value");
                Double confidence = Double.valueOf(row.getAs("Confidence").toString());
                String provenance = row.getAs("Provenance");
                String pid = row.getAs("PID");

                // Update only if confidence is higher
                if (!valueMap.containsKey(value) || valueMap.get(value).confidence < confidence) {
                    valueMap.put(value, new Tuple(confidence, provenance, pid));
                }
            }
        }

        List<Row> result = new ArrayList<>();
        for (Map.Entry<String, Tuple> entry : valueMap.entrySet()) {
            Tuple t = entry.getValue();
            result.add(RowFactory.create(t.provenance, t.pid, entry.getKey(), t.confidence, "active"));
        }

        return result;
    }

    // Helper class for holding multiple values
    private static class Tuple {
        double confidence;
        String provenance;
        String pid;

        public Tuple(double confidence, String provenance, String pid) {
            this.confidence = confidence;
            this.provenance = provenance;
            this.pid = pid;
        }
    }

}

