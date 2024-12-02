package eu.dnetlib.dhp.sx.bio.pubmed;

public class PMIdentifier {

    private String pid;
    private String type;


    public PMIdentifier(String pid, String type) {
        this.pid = cleanPid(pid);
        this.type = type;
    }

    public PMIdentifier() {

    }

    private String cleanPid(String pid) {

        if (pid == null) {
            return null;
        }

        // clean ORCID ID in the form 0000000163025705 to 0000-0001-6302-5705
        if (pid.matches("[0-9]{15}[0-9X]")) {
            return pid.replaceAll("(.{4})(.{4})(.{4})(.{4})", "$1-$2-$3-$4");
        }

        // clean ORCID in the form http://orcid.org/0000-0001-8567-3543 to 0000-0001-8567-3543
        if (pid.matches("http://orcid.org/[0-9]{4}-[0-9]{4}-[0-9]{4}-[0-9]{4}")) {
            return pid.replaceAll("http://orcid.org/", "");
        }
        return pid;
    }

    public String getPid() {
        return pid;
    }

    public PMIdentifier setPid(String pid) {
        this.pid = cleanPid(pid);
        return this;
    }

    public String getType() {
        return type;
    }

    public PMIdentifier setType(String type) {
        this.type = type;
        return this;
    }
}
