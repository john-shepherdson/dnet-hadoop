
package eu.dnetlib.dhp.schema.dump.oaf.graph;

import java.io.Serializable;

public class Funder implements Serializable {

	// private String id ;

	private String shortName;

	private String name;

	// private List<Levels> funding_levels;
	private Fundings funding_stream;

	private String jurisdiction;

//    public String getId() {
//        return id;
//    }
//
//    public void setId(String id) {
//        this.id = id;
//    }

	public String getShortName() {
		return shortName;
	}

	public void setShortName(String shortName) {
		this.shortName = shortName;
	}

	public String getName() {
		return name;
	}

	public void setName(String name) {
		this.name = name;
	}

//    public List<Levels> getFunding_levels() {
//        return funding_levels;
//    }
//
//    public void setFunding_levels(List<Levels> funding_levels) {
//        this.funding_levels = funding_levels;
//    }

	public String getJurisdiction() {
		return jurisdiction;
	}

	public void setJurisdiction(String jurisdiction) {
		this.jurisdiction = jurisdiction;
	}

	public Fundings getFunding_stream() {
		return funding_stream;
	}

	public void setFunding_stream(Fundings funding_stream) {
		this.funding_stream = funding_stream;
	}
}
