
package eu.dnetlib.dhp.api;


import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

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
		String body = QueryCommunityAPI.communities(true);
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
		String body = QueryCommunityAPI.community(id, true);
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
		String body = QueryCommunityAPI.communityDatasource(id, true);
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
		CommunityConfiguration cc = Utils.getCommunityConfiguration(true);
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
	void eutopiaCommunityConfiguration() throws Exception {
		CommunityConfiguration cc = Utils.getCommunityConfiguration(true);
		System.out.println(cc.getCommunities().keySet());
		Community community = cc.getCommunities().get("eutopia");
		community.getProviders().forEach(p -> System.out.println(p.getOpenaireId()));
	}

	@Test
	void getCommunityProjects() throws Exception {
		CommunityEntityMap projectMap = Utils.getCommunityProjects(true);

		Assertions
			.assertTrue(
				projectMap
					.keySet()
					.stream()
					.allMatch(k -> k.startsWith("40|")));

		System.out.println(projectMap);
	}

	@Test
	void getCommunityOrganizations() throws Exception {
		CommunityEntityMap organizationMap = Utils.getCommunityOrganization(true);
		Assertions.assertTrue(organizationMap.keySet().stream().allMatch(k -> k.startsWith("20|")));

	}

}
