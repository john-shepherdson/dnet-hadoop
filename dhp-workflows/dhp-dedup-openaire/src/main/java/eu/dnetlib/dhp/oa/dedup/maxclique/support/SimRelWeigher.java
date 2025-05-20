package eu.dnetlib.dhp.oa.dedup.maxclique.support;

import eu.dnetlib.dhp.oa.dedup.maxclique.Weigher;
import eu.dnetlib.pace.config.DedupConfig;
import eu.dnetlib.pace.tree.support.TreeProcessor;
import org.apache.spark.sql.Row;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

public class SimRelWeigher implements Weigher<Row> {

    TreeProcessor treeProcessor;

    public SimRelWeigher(DedupConfig dedupConfig) {
        treeProcessor = new TreeProcessor(dedupConfig);
    }

    @Override
    public double weigh(Row a, Row b) {

        List<String> negativeConstraintsA = Arrays.asList(a.schema().fieldNames()).contains("negativeConstraints")? a.getList(a.fieldIndex("negativeConstraints")) : Collections.emptyList();
        List<String> negativeConstraintsB = Arrays.asList(b.schema().fieldNames()).contains("negativeConstraints")? b.getList(b.fieldIndex("negativeConstraints")) : Collections.emptyList();

        if (negativeConstraintsA.isEmpty() && negativeConstraintsB.isEmpty() && !Collections.disjoint(negativeConstraintsA, negativeConstraintsB)) {
            return -1;
        }

        return treeProcessor.computeScore(a, b);
    }
}
