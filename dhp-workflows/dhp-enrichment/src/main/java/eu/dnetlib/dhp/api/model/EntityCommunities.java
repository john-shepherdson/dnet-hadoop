
package eu.dnetlib.dhp.api.model;

import java.io.Serializable;
import java.util.List;

import eu.dnetlib.dhp.schema.common.ModelSupport;
import eu.dnetlib.dhp.schema.oaf.Datasource;

/**
 * @author miriam.baglioni
 * @Date 13/02/24
 */
public class EntityCommunities implements Serializable {
	private String entityId;
	private List<String> communitiesId;

	public String getEntityId() {
		return entityId;
	}

	public void setEntityId(String entityId) {
		this.entityId = entityId;
	}

	public List<String> getCommunitiesId() {
		return communitiesId;
	}

	public void setCommunitiesId(List<String> communitiesId) {
		this.communitiesId = communitiesId;
	}

	public static EntityCommunities newInstance(String dsid, List<String> csid) {
		EntityCommunities dsc = new EntityCommunities();
		dsc.entityId = dsid;
		dsc.communitiesId = csid;
		return dsc;
	}
}
