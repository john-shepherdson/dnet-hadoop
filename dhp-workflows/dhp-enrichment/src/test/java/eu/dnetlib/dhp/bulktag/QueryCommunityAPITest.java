
package eu.dnetlib.dhp.bulktag;

import java.util.List;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

import eu.dnetlib.dhp.api.QueryCommunityAPI;
import eu.dnetlib.dhp.api.Utils;
import eu.dnetlib.dhp.api.model.CommunityEntityMap;
import eu.dnetlib.dhp.api.model.CommunityModel;
import eu.dnetlib.dhp.api.model.CommunitySummary;
import eu.dnetlib.dhp.api.model.DatasourceList;
import eu.dnetlib.dhp.bulktag.community.Community;
import eu.dnetlib.dhp.bulktag.community.CommunityConfiguration;

/**
 * @author miriam.baglioni
 * @Date 06/10/23
 */
public class QueryCommunityAPITest {

	@Test
	void communityList() throws Exception {
		String body = QueryCommunityAPI.communities();
		new ObjectMapper()
			.readValue(body, CommunitySummary.class)
			.forEach(p -> {
				try {
					System.out.println(new ObjectMapper().writeValueAsString(p));
				} catch (JsonProcessingException e) {
					throw new RuntimeException(e);
				}
			});
	}

	@Test
	void community() throws Exception {
		String id = "dh-ch";
		String body = QueryCommunityAPI.community(id);
		System.out
			.println(
				new ObjectMapper()
					.writeValueAsString(
						new ObjectMapper()
							.readValue(body, CommunityModel.class)));
	}

	@Test
	void communityDatasource() throws Exception {
		String id = "dh-ch";
		String body = QueryCommunityAPI.communityDatasource(id);
		new ObjectMapper()
			.readValue(body, DatasourceList.class)
			.forEach(ds -> {
				try {
					System.out.println(new ObjectMapper().writeValueAsString(ds));
				} catch (JsonProcessingException e) {
					throw new RuntimeException(e);
				}
			});
		;
	}

	@Test
	void validCommunities() throws Exception {
		CommunityConfiguration cc = Utils.getCommunityConfiguration();
		System.out.println(cc.getCommunities().keySet());
		Community community = cc.getCommunities().get("aurora");
		Assertions.assertEquals(0, community.getSubjects().size());
		Assertions.assertEquals(null, community.getConstraints());
		Assertions.assertEquals(null, community.getRemoveConstraints());
		Assertions.assertEquals(2, community.getZenodoCommunities().size());
		Assertions
			.assertTrue(
				community.getZenodoCommunities().stream().anyMatch(c -> c.equals("aurora-universities-network")));
		Assertions
			.assertTrue(community.getZenodoCommunities().stream().anyMatch(c -> c.equals("university-of-innsbruck")));
		Assertions.assertEquals(35, community.getProviders().size());
		Assertions
			.assertEquals(
				35, community.getProviders().stream().filter(p -> p.getSelectionConstraints() == null).count());
	}

	@Test
	void getCommunityProjects() throws Exception {
		CommunityEntityMap projectMap = Utils.getCommunityProjects();
		Assertions.assertFalse(projectMap.containsKey("mes"));
		Assertions.assertEquals(33, projectMap.size());
		Assertions
			.assertTrue(
				projectMap
					.keySet()
					.stream()
					.allMatch(k -> projectMap.get(k).stream().allMatch(p -> p.startsWith("40|"))));
	}

}
