package eu.dnetlib.dhp;

import com.google.gson.Gson;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by miriam on 01/08/2018.
 */
public class Community {

    private static final Log log = LogFactory.getLog(Community.class);

    private String id;
    private List<String> subjects = new ArrayList<>();
    private List<Datasource> datasources = new ArrayList<>();
    private List<ZenodoCommunity> zenodoCommunities = new ArrayList<>();


    public String toJson() {
        final Gson g = new Gson();
        return g.toJson(this);
    }

    public boolean isValid() {
        return !getSubjects().isEmpty() || !getDatasources().isEmpty() || !getZenodoCommunities().isEmpty();
    }

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public List<String> getSubjects() {
        return subjects;
    }

    public void setSubjects(List<String> subjects) {
        this.subjects = subjects;
    }

    public List<Datasource> getDatasources() {
        return datasources;
    }

    public void setDatasources(List<Datasource> datasources) {
        this.datasources = datasources;
    }

    public List<ZenodoCommunity> getZenodoCommunities() {
        return zenodoCommunities;
    }

    public void setZenodoCommunities(List<ZenodoCommunity> zenodoCommunities) {
        this.zenodoCommunities = zenodoCommunities;
    }

}
