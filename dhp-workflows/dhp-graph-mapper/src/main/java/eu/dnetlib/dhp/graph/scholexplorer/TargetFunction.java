package eu.dnetlib.dhp.graph.scholexplorer;


import eu.dnetlib.dhp.schema.oaf.Relation;
import org.apache.commons.lang3.StringUtils;
import org.apache.spark.api.java.function.MapFunction;

public class TargetFunction implements MapFunction<Relation, Relation> {
    @Override
    public Relation call(Relation relation) throws Exception {
        final String type = StringUtils.substringBefore(relation.getSource(), "|");
        relation.setTarget(String.format("%s|%s", type, StringUtils.substringAfter(relation.getTarget(),"::")));
        return relation;
    }
}
