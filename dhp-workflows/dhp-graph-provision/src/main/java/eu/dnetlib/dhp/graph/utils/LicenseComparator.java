package eu.dnetlib.dhp.graph.utils;

import eu.dnetlib.dhp.schema.oaf.Qualifier;

import java.util.Comparator;

public class LicenseComparator implements Comparator<Qualifier> {

    @Override
    public int compare(Qualifier left, Qualifier right) {

        if (left == null && right == null) return 0;
        if (left == null) return 1;
        if (right == null) return -1;

        String lClass = left.getClassid();
        String rClass = right.getClassid();

        if (lClass.equals(rClass)) return 0;

        if (lClass.equals("OPEN SOURCE")) return -1;
        if (rClass.equals("OPEN SOURCE")) return 1;

        if (lClass.equals("OPEN")) return -1;
        if (rClass.equals("OPEN")) return 1;

        if (lClass.equals("6MONTHS")) return -1;
        if (rClass.equals("6MONTHS")) return 1;

        if (lClass.equals("12MONTHS")) return -1;
        if (rClass.equals("12MONTHS")) return 1;

        if (lClass.equals("EMBARGO")) return -1;
        if (rClass.equals("EMBARGO")) return 1;

        if (lClass.equals("RESTRICTED")) return -1;
        if (rClass.equals("RESTRICTED")) return 1;

        if (lClass.equals("CLOSED")) return -1;
        if (rClass.equals("CLOSED")) return 1;

        if (lClass.equals("UNKNOWN")) return -1;
        if (rClass.equals("UNKNOWN")) return 1;

        // Else (but unlikely), lexicographical ordering will do.
        return lClass.compareTo(rClass);
    }

}
