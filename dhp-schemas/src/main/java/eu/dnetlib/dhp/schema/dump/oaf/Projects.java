package eu.dnetlib.dhp.schema.dump.oaf;

import eu.dnetlib.dhp.schema.oaf.Project;

import java.util.List;

public class Projects {

    private String id ;//OpenAIRE id
    private String code;

    private String acronym;

    private String title;

    private List<String> funding_tree;

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public String getCode() {
        return code;
    }

    public void setCode(String code) {
        this.code = code;
    }

    public String getAcronym() {
        return acronym;
    }

    public void setAcronym(String acronym) {
        this.acronym = acronym;
    }

    public String getTitle() {
        return title;
    }

    public void setTitle(String title) {
        this.title = title;
    }

    public List<String> getFunding_tree() {
        return funding_tree;
    }

    public void setFunding_tree(List<String> funding_tree) {
        this.funding_tree = funding_tree;
    }

    public static Projects newInstance(String id, String code, String acronym, String title, List<String> funding_tree){
        Projects projects = new Projects();
        projects.setAcronym(acronym);
        projects.setCode(code);
        projects.setFunding_tree(funding_tree);
        projects.setId(id);
        projects.setTitle(title);
        return projects;
    }
}
