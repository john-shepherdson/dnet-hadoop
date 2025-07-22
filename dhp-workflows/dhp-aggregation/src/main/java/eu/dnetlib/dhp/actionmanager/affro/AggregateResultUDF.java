package eu.dnetlib.dhp.actionmanager.affro;

import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.api.java.UDF1;
import scala.collection.mutable.WrappedArray;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class AggregateResultUDF implements UDF1<WrappedArray<Row>, Row> {

    @Override
    public Row call(WrappedArray<Row> group) {
        List<Row> authors = new ArrayList<>();
        List<List<Row>> allMatchings = new ArrayList<>();
        String id = null;

        for (int i = 0; i < group.length(); i++) {
            Row entry = group.apply(i);

            if (id == null) id = entry.getAs("id");

            // author è uno struct con First, Last, Full, orcid
            Row authorRow = entry.getAs("author");
            String first = authorRow.getAs("First");
            String last = authorRow.getAs("Last");
            String full = authorRow.getAs("Full");
            String orcid = authorRow.getAs("orcid");
            Row name = RowFactory.create(first, last, full, orcid);

            // Raw_affiliation è una lista
            List<String> affiliations = entry.getList(entry.fieldIndex("Raw_affiliation"));

            // Matchings: WrappedArray<Row> → List<Row>
            WrappedArray<Row> matchArray = entry.getAs("Matchings");
            List<Row> matchList = new ArrayList<>();
            for (int j = 0; j < matchArray.length(); j++) {
                matchList.add(matchArray.apply(j));
            }

            List<Row> amatch = getMatchings(matchList);
            allMatchings.add(amatch);

            Row author = RowFactory.create(name, null, null, affiliations, amatch);
            authors.add(author);
        }

        List<Row> organizations = regroupAndSelectDistinctMatch(allMatchings);
        return RowFactory.create(id, authors, organizations);
    }

    private List<Row> getMatchings(List<Row> input) {
        // Puoi personalizzare la logica, o passarla come riferimento
        return input; // stub iniziale
    }

    private List<Row> regroupAndSelectDistinctMatch(List<List<Row>> inputGroups) {
        Map<String, Tuple> valueMap = new HashMap<>();

        for (List<Row> group : inputGroups) {
            for (Row row : group) {
                if (!"active".equalsIgnoreCase(row.getAs("Status"))) continue;

                String value = row.getAs("Value");
                Double confidence = Double.valueOf(row.getAs("Confidence").toString());
                String provenance = row.getAs("Provenance");
                String pid = row.getAs("PID");

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

