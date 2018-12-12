package eu.dnetlib.pace.tree;

import eu.dnetlib.pace.model.Field;
import eu.dnetlib.pace.model.FieldList;

import java.util.List;
import java.util.Map;

@TreeNodeClass("coauthorsMatch")
public class CoauthorsMatch extends AbstractTreeNode {

    public CoauthorsMatch(Map<String, Number> params) {
        super(params);
    }

    @Override
    public int compare(Field a, Field b) {

        final List<String> c1 = ((FieldList) a).stringList();
        final List<String> c2 = ((FieldList) b).stringList();

        int size1 = c1.size();
        int size2 = c2.size();

        //few coauthors or too many coauthors
        if (size1 < params.getOrDefault("minCoauthors", 5).intValue() || size2 < params.getOrDefault("minCoauthors", 5).intValue() || (size1+size2 > params.getOrDefault("maxCoauthors", 200).intValue()))
            return 0;

        int coauthorship = 0;
        for (String ca1: c1){

            for (String ca2: c2){

                if (stringSimilarity(ca1.replaceAll("\\.","").replaceAll(" ",""), ca2.replaceAll("\\.","").replaceAll(" ",""))>= params.getOrDefault("simTh", 0.7).doubleValue())
                    coauthorship++;
            }
        }

        if (coauthorship>=params.getOrDefault("th", 5).intValue())
            return 1;
        else if (coauthorship == 0)
            return -1;
        else
            return 0;
    }
}
