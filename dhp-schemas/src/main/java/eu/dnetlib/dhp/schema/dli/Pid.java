package eu.dnetlib.dhp.schema.dli;

import eu.dnetlib.dhp.utils.DHPUtils;
import org.apache.commons.lang3.StringUtils;

public class Pid {

    private String pid;

    private String pidType;

    public String getPid() {
        return pid;
    }

    public void setPid(String pid) {
        this.pid = pid;
    }

    public String getPidType() {
        return pidType;
    }

    public void setPidType(String pidType) {
        this.pidType = pidType;
    }

    public String generateId() {
        if(StringUtils.isEmpty(pid) || StringUtils.isEmpty(pidType))
            return null;
        return DHPUtils.md5(String.format("%s::%s", pid, pidType));
    }
}
