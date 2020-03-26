package eu.dnetlib.dhp.schema.scholexplorer;

import eu.dnetlib.dhp.schema.oaf.OafEntity;
import eu.dnetlib.dhp.schema.oaf.Publication;
import org.apache.commons.lang3.StringUtils;
import java.io.Serializable;
import java.util.*;

public class DLIPublication extends Publication implements Serializable {

    private String originalObjIdentifier;

    private List<ProvenaceInfo> dlicollectedfrom;

    private String completionStatus;

    public String getCompletionStatus() {
        return completionStatus;
    }

    public void setCompletionStatus(String completionStatus) {
        this.completionStatus = completionStatus;
    }

    public List<ProvenaceInfo> getDlicollectedfrom() {
        return dlicollectedfrom;
    }

    public void setDlicollectedfrom(List<ProvenaceInfo> dlicollectedfrom) {
        this.dlicollectedfrom = dlicollectedfrom;
    }

    public String getOriginalObjIdentifier() {
        return originalObjIdentifier;
    }

    public void setOriginalObjIdentifier(String originalObjIdentifier) {
        this.originalObjIdentifier = originalObjIdentifier;
    }

    @Override
    public void mergeFrom(OafEntity e) {
        super.mergeFrom(e);
        DLIPublication p = (DLIPublication) e;
        if (StringUtils.isBlank(completionStatus) && StringUtils.isNotBlank(p.completionStatus))
            completionStatus = p.completionStatus;
        if ("complete".equalsIgnoreCase(p.completionStatus))
            completionStatus = "complete";
        dlicollectedfrom = mergeProvenance(dlicollectedfrom, p.getDlicollectedfrom());
    }

    private List<ProvenaceInfo> mergeProvenance(final List<ProvenaceInfo> a, final List<ProvenaceInfo> b) {
        Map<String, ProvenaceInfo> result = new HashMap<>();
        if (a != null)
            a.forEach(p -> {
                if (p != null && StringUtils.isNotBlank(p.getId()) && result.containsKey(p.getId())) {
                    if ("incomplete".equalsIgnoreCase(result.get(p.getId()).getCompletionStatus()) && StringUtils.isNotBlank(p.getCompletionStatus())) {
                        result.put(p.getId(), p);
                    }

                } else if (p != null && p.getId() != null && !result.containsKey(p.getId()))
                    result.put(p.getId(), p);
            });
        if (b != null)
            b.forEach(p -> {
                if (p != null && StringUtils.isNotBlank(p.getId()) && result.containsKey(p.getId())) {
                    if ("incomplete".equalsIgnoreCase(result.get(p.getId()).getCompletionStatus()) && StringUtils.isNotBlank(p.getCompletionStatus())) {
                        result.put(p.getId(), p);
                    }

                } else if (p != null && p.getId() != null && !result.containsKey(p.getId()))
                    result.put(p.getId(), p);
            });

        return new ArrayList<>(result.values());
    }
}
