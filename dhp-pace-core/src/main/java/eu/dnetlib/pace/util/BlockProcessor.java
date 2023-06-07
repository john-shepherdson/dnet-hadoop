package eu.dnetlib.pace.util;

import eu.dnetlib.pace.config.DedupConfig;
import eu.dnetlib.pace.config.WfConfig;
import eu.dnetlib.pace.tree.support.TreeProcessor;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.types.ArrayType;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.StringType;
import org.apache.spark.sql.types.StructType;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;

public class BlockProcessor {

    public static final List<String> accumulators= new ArrayList<>();

    private static final Log log = LogFactory.getLog(BlockProcessor.class);

    private DedupConfig dedupConf;

    private final int identifierFieldPos;
    private final int orderFieldPos;

    public static void constructAccumulator( final DedupConfig dedupConf) {
        accumulators.add(String.format("%s::%s",dedupConf.getWf().getEntityType(), "records per hash key = 1"));
        accumulators.add(String.format("%s::%s",dedupConf.getWf().getEntityType(), "missing " + dedupConf.getWf().getOrderField()));
        accumulators.add(String.format("%s::%s",dedupConf.getWf().getEntityType(), String.format("Skipped records for count(%s) >= %s", dedupConf.getWf().getOrderField(), dedupConf.getWf().getGroupMaxSize())));
        accumulators.add(String.format("%s::%s",dedupConf.getWf().getEntityType(), "skip list"));
        accumulators.add(String.format("%s::%s",dedupConf.getWf().getEntityType(), "dedupSimilarity (x2)"));
        accumulators.add(String.format("%s::%s",dedupConf.getWf().getEntityType(), "d < " + dedupConf.getWf().getThreshold()));
    }

    public BlockProcessor(DedupConfig dedupConf, int identifierFieldPos, int orderFieldPos) {
        this.dedupConf = dedupConf;
        this.identifierFieldPos = identifierFieldPos;
        this.orderFieldPos = orderFieldPos;
    }

    public void processSortedRows(final Collection<Row> documents, final Reporter context)  {
        if (documents.size() > 1) {
//            log.info("reducing key: '" + key + "' records: " + q.size());
            processRows(documents, context);

        } else {
            context.incrementCounter(dedupConf.getWf().getEntityType(), "records per hash key = 1", 1);
        }
    }


    private void processRows(final Collection<Row> queue, final Reporter context)  {

        Iterator<Row> it = queue.iterator();
        while (it.hasNext()) {

            final Row pivot = it.next();
            it.remove();


            final String idPivot = pivot.getString(identifierFieldPos); //identifier
            final Object fieldsPivot = getJavaValue(pivot, orderFieldPos);
            final String fieldPivot = (fieldsPivot == null) ? "" : fieldsPivot.toString();
            final WfConfig wf = dedupConf.getWf();

            if (fieldPivot != null) {
                int i = 0;
                for (final Row curr : queue) {
                    final String idCurr = curr.getString(identifierFieldPos); //identifier

                    if (mustSkip(idCurr)) {

                        context.incrementCounter(wf.getEntityType(), "skip list", 1);

                        break;
                    }

                    if (i > wf.getSlidingWindowSize()) {
                        break;
                    }

                    final Object fieldsCurr = getJavaValue(curr, orderFieldPos);
                    final String fieldCurr = (fieldsCurr == null) ? null : fieldsCurr.toString();

                    if (!idCurr.equals(idPivot) && (fieldCurr != null)) {

                        final TreeProcessor treeProcessor = new TreeProcessor(dedupConf);

                        emitOutput(treeProcessor.compare(pivot, curr), idPivot, idCurr, context);

                    }
                }
            }
        }
    }

    public Object getJavaValue(Row row, int pos) {
        DataType dt = row.schema().fields()[pos].dataType();
        if (dt instanceof StringType) {
            return row.getString(pos);
        } else if (dt instanceof ArrayType) {
            return row.getList(pos);
        }

        return null;
    }

    private void emitOutput(final boolean result, final String idPivot, final String idCurr, final Reporter context)  {

        if (result) {
            writeSimilarity(context, idPivot, idCurr);
            context.incrementCounter(dedupConf.getWf().getEntityType(), "dedupSimilarity (x2)", 1);
        } else {
            context.incrementCounter(dedupConf.getWf().getEntityType(), "d < " + dedupConf.getWf().getThreshold(), 1);
        }
    }

    private boolean mustSkip(final String idPivot) {
        return dedupConf.getWf().getSkipList().contains(getNsPrefix(idPivot));
    }

    private String getNsPrefix(final String id) {
        return StringUtils.substringBetween(id, "|", "::");
    }

    private void writeSimilarity(final Reporter context, final String from, final String to)  {
        final String type = dedupConf.getWf().getEntityType();

        context.emit(type, from, to);
        context.emit(type, to, from);
    }

}
