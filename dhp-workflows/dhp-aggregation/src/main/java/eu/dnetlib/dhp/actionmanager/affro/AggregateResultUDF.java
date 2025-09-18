package eu.dnetlib.dhp.actionmanager.affro;

import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.api.java.UDF1;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;
import scala.collection.mutable.WrappedArray;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.apache.spark.sql.types.DataTypes.StringType;

public class AggregateResultUDF implements UDF1<WrappedArray<Row>, Row> {

    @Override
    public Row call(WrappedArray<Row> group) {
        List<Row> authors = new ArrayList<>();
        List<List<Row>> allMatchings = new ArrayList<>();
        String id = null;

        for (int i = 0; i < group.length(); i++) {
            Row entry = group.apply(i);
            String authorName = entry.getAs("fullname");

            // affiliations: WrappedArray<Row> → List<Row>
            WrappedArray<Row> affArray = entry.getAs("affiliations");
            Row author = RowFactory.create(authorName, affArray, entry.getAs("corresponding"), entry.getAs("contributor_roles"), entry.getAs("pids"));
            authors.add(author);
            if (id == null) id = entry.getAs("id");
            List<Row> matchList = new ArrayList<>();
            for (int j = 0; j < affArray.length(); j++) {
                WrappedArray<Row> matchings = affArray.apply(j).getAs("matchings");
                for (int k = 0; k < matchings.length(); k++) {
                    matchList.add(matchings.apply(k));
                }
            }


            allMatchings.add(matchList);

        }

        List<Row> organizations = regroupAndSelectDistinctMatch(allMatchings);
        return RowFactory.create(id, authors, organizations);
    }


    private List<Row> regroupAndSelectDistinctMatch(List<List<Row>> inputGroups) {
        Map<String, Tuple> valueMap = new HashMap<>();

        for (List<Row> group : inputGroups) {
            for (Row row : group) {
                if (!"active".equalsIgnoreCase(row.getAs("status"))) continue;

                String value = row.getAs("value");
                Double confidence = Double.valueOf(row.getAs("confidence").toString());
                String provenance = row.getAs("provenance");
                String pid = row.getAs("pid");
                String country = row.getAs("country");
                String name = row.getAs("name");

                if (!valueMap.containsKey(value) || valueMap.get(value).confidence < confidence) {
                    valueMap.put(value, new Tuple(confidence, provenance, pid, country, name));
                }
            }
        }

        List<Row> result = new ArrayList<>();
        for (Map.Entry<String, Tuple> entry : valueMap.entrySet()) {
            Tuple t = entry.getValue();
            result.add(RowFactory.create(t.provenance, t.pid, entry.getKey(), t.confidence, "active", t.country, t.name));
        }

        return result;
    }

    private static class Tuple {
        double confidence;
        String provenance;
        String pid;
        String country;
        String name;

        public Tuple(double confidence, String provenance, String pid, String country, String name) {
            this.confidence = confidence;
            this.provenance = provenance;
            this.pid = pid;
            this.country = country;
            this.name = name;
        }
    }
}

