package eu.dnetlib.dhp.actionmanager.raid.model;

import java.io.Serializable;
import java.util.List;

public class RAiDEntity implements Serializable {

    String raid;
    List<String> authors;
    String startDate;
    String endDate;
    List<String> subjects;
    List<String> titles;
    List<String> ids;
    String title;
    String summary;

    public RAiDEntity(){}
    public RAiDEntity(String raid, List<String> authors, String startDate, String endDate, List<String> subjects, List<String> titles, List<String> ids, String title, String summary) {
        this.raid = raid;
        this.authors = authors;
        this.startDate = startDate;
        this.endDate = endDate;
        this.subjects = subjects;
        this.titles = titles;
        this.ids = ids;
        this.title = title;
        this.summary = summary;
    }

    public String getRaid() {
        return raid;
    }

    public void setRaid(String raid) {
        this.raid = raid;
    }

    public List<String> getAuthors() {
        return authors;
    }

    public void setAuthors(List<String> authors) {
        this.authors = authors;
    }

    public String getStartDate() {
        return startDate;
    }

    public void setStartDate(String startDate) {
        this.startDate = startDate;
    }

    public String getEndDate() {
        return endDate;
    }

    public void setEndDate(String endDate) {
        this.endDate = endDate;
    }

    public List<String> getSubjects() {
        return subjects;
    }

    public void setSubjects(List<String> subjects) {
        this.subjects = subjects;
    }

    public List<String> getTitles() {
        return titles;
    }

    public void setTitles(List<String> titles) {
        this.titles = titles;
    }

    public List<String> getIds() {
        return ids;
    }

    public void setIds(List<String> ids) {
        this.ids = ids;
    }

    public String getTitle() {
        return title;
    }

    public void setTitle(String title) {
        this.title = title;
    }

    public String getSummary() {
        return summary;
    }

    public void setSummary(String summary) {
        this.summary = summary;
    }
}
