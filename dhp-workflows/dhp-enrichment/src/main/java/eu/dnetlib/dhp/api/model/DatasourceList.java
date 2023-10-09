package eu.dnetlib.dhp.api.model;

import eu.dnetlib.dhp.api.model.CommunityContentprovider;


import java.io.Serializable;
import java.util.ArrayList;
public class DatasourceList extends ArrayList<CommunityContentprovider> implements Serializable {
	public DatasourceList(){
		super();
	}
}