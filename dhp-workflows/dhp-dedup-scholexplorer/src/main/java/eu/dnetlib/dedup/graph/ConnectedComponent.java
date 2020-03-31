package eu.dnetlib.dedup.graph;

import com.fasterxml.jackson.databind.ObjectMapper;
import eu.dnetlib.dedup.DedupUtility;
import eu.dnetlib.pace.util.PaceException;
import org.apache.commons.lang.StringUtils;
import org.codehaus.jackson.annotate.JsonIgnore;

import java.io.IOException;
import java.io.Serializable;
import java.util.Set;

public class ConnectedComponent implements Serializable {

    private Set<String> docIds;
    private String ccId;


    public ConnectedComponent() {
    }

    public ConnectedComponent(Set<String> docIds) {
        this.docIds = docIds;
        createID();
    }

    public String createID() {
        if (docIds.size() > 1) {
            final String s = getMin();
            String prefix = s.split("\\|")[0];
            ccId =prefix + "|dedup_______::" + DedupUtility.md5(s);
            return ccId;
        } else {
            return docIds.iterator().next();
        }
    }

    @JsonIgnore
    public String getMin(){

        final StringBuilder min = new StringBuilder();
        docIds.forEach(i -> {
            if (StringUtils.isBlank(min.toString())) {
                min.append(i);
            } else {
                if (min.toString().compareTo(i) > 0) {
                    min.setLength(0);
                    min.append(i);
                }
            }
        });
        return min.toString();
    }

    @Override
    public String toString(){
        ObjectMapper mapper = new ObjectMapper();
        try {
            return mapper.writeValueAsString(this);
        } catch (IOException e) {
            throw new PaceException("Failed to create Json: ", e);
        }
    }

    public Set<String> getDocIds() {
        return docIds;
    }

    public void setDocIds(Set<String> docIds) {
        this.docIds = docIds;
    }

    public String getCcId() {
        return ccId;
    }

    public void setCcId(String ccId) {
        this.ccId = ccId;
    }
}
