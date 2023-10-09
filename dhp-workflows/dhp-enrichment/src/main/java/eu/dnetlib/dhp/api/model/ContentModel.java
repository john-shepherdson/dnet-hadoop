package eu.dnetlib.dhp.api.model;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;

import java.io.Serializable;
import java.util.List;

/**
 * @author miriam.baglioni
 * @Date 09/10/23
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public class ContentModel implements Serializable {
    private List<ProjectModel> content;
       private Integer totalPages;
       private Boolean last;
       private Integer number;

    public List<ProjectModel> getContent() {
        return content;
    }

    public void setContent(List<ProjectModel> content) {
        this.content = content;
    }

    public Integer getTotalPages() {
        return totalPages;
    }

    public void setTotalPages(Integer totalPages) {
        this.totalPages = totalPages;
    }

    public Boolean getLast() {
        return last;
    }

    public void setLast(Boolean last) {
        this.last = last;
    }

    public Integer getNumber() {
        return number;
    }

    public void setNumber(Integer number) {
        this.number = number;
    }
}
