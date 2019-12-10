package eu.dnetlib.dedup;
import com.google.common.collect.ComparisonChain;
import java.io.Serializable;
import java.util.Comparator;

public class OafComparator implements Comparator<OafKey>, Serializable {

    @Override
    public int compare(OafKey a, OafKey b) {
        return ComparisonChain.start()
                .compare(a.getDedupId(), b.getDedupId())
                .compare(a.getTrust(), b.getTrust())
                .result();
    }
}