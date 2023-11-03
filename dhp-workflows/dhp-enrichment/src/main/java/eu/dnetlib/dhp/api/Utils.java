
package eu.dnetlib.dhp.api;

import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.amazonaws.util.StringUtils;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.Maps;

import eu.dnetlib.dhp.api.model.*;
import eu.dnetlib.dhp.bulktag.community.Community;
import eu.dnetlib.dhp.bulktag.community.CommunityConfiguration;
import eu.dnetlib.dhp.bulktag.community.Provider;
import eu.dnetlib.dhp.bulktag.criteria.VerbResolver;
import eu.dnetlib.dhp.bulktag.criteria.VerbResolverFactory;

/**
 * @author miriam.baglioni
 * @Date 09/10/23
 */
public class Utils implements Serializable {
	private static final ObjectMapper MAPPER = new ObjectMapper();
	private static final VerbResolver resolver = VerbResolverFactory.newInstance();

	private static final Logger log = LoggerFactory.getLogger(Utils.class);

	public static CommunityConfiguration getCommunityConfiguration(boolean production) throws IOException {
		final Map<String, Community> communities = Maps.newHashMap();
		List<Community> validCommunities = new ArrayList<>();
		getValidCommunities(production)
			.forEach(community -> {
				try {
					CommunityModel cm = MAPPER
						.readValue(QueryCommunityAPI.community(community.getId(), production), CommunityModel.class);
					validCommunities.add(getCommunity(cm));
				} catch (IOException e) {
					throw new RuntimeException(e);
				}
			});
		validCommunities.forEach(community -> {
			try {
				DatasourceList dl = MAPPER
					.readValue(
						QueryCommunityAPI.communityDatasource(community.getId(), production), DatasourceList.class);
				community.setProviders(dl.stream().map(d -> {
					if (d.getEnabled() == null || Boolean.FALSE.equals(d.getEnabled()))
						return null;
					Provider p = new Provider();
					p.setOpenaireId("10|" + d.getOpenaireId());
					p.setSelectionConstraints(d.getSelectioncriteria());
					if (p.getSelectionConstraints() != null)
						p.getSelectionConstraints().setSelection(resolver);
					return p;
				})
					.filter(Objects::nonNull)
					.collect(Collectors.toList()));
			} catch (IOException e) {
				throw new RuntimeException(e);
			}
		});

		validCommunities.forEach(community -> {
			if (community.isValid())
				communities.put(community.getId(), community);
		});
		return new CommunityConfiguration(communities);
	}

	private static Community getCommunity(CommunityModel cm) {
		Community c = new Community();
		c.setId(cm.getId());
		c.setZenodoCommunities(cm.getOtherZenodoCommunities());
		if (!StringUtils.isNullOrEmpty(cm.getZenodoCommunity()))
			c.getZenodoCommunities().add(cm.getZenodoCommunity());
		c.setSubjects(cm.getSubjects());
		c.getSubjects().addAll(cm.getFos());
		c.getSubjects().addAll(cm.getSdg());
		if (cm.getAdvancedConstraints() != null) {
			c.setConstraints(cm.getAdvancedConstraints());
			c.getConstraints().setSelection(resolver);
		}
		if (cm.getRemoveConstraints() != null) {
			c.setRemoveConstraints(cm.getRemoveConstraints());
			c.getRemoveConstraints().setSelection(resolver);
		}
		return c;
	}

	public static List<CommunityModel> getValidCommunities(boolean production) throws IOException {
		return MAPPER
			.readValue(QueryCommunityAPI.communities(production), CommunitySummary.class)
			.stream()
			.filter(
				community -> !community.getStatus().equals("hidden") &&
					(community.getType().equals("ri") || community.getType().equals("community")))
			.collect(Collectors.toList());
	}

	/**
	 * it returns for each organization the list of associated communities
	 */
	public static CommunityEntityMap getCommunityOrganization(boolean production) throws IOException {
		CommunityEntityMap organizationMap = new CommunityEntityMap();
		getValidCommunities(production)
			.forEach(community -> {
				String id = community.getId();
				try {
					List<String> associatedOrgs = MAPPER
						.readValue(
							QueryCommunityAPI.communityPropagationOrganization(id, production), OrganizationList.class);
					associatedOrgs.forEach(o -> {
						if (!organizationMap
							.keySet()
							.contains(
								"20|" + o))
							organizationMap.put("20|" + o, new ArrayList<>());
						organizationMap.get("20|" + o).add(community.getId());
					});
				} catch (IOException e) {
					throw new RuntimeException(e);
				}
			});

		return organizationMap;
	}

	public static CommunityEntityMap getCommunityProjects(boolean production) throws IOException {
		CommunityEntityMap projectMap = new CommunityEntityMap();
		getValidCommunities(production)
			.forEach(community -> {
				int page = -1;
				int size = 100;
				ContentModel cm = new ContentModel();
				do {
					page++;
					try {
						cm = MAPPER
							.readValue(
								QueryCommunityAPI
									.communityProjects(
										community.getId(), String.valueOf(page), String.valueOf(size), production),
								ContentModel.class);
						if (cm.getContent().size() > 0) {
							cm.getContent().forEach(p -> {
								if (!projectMap.keySet().contains("40|" + p.getOpenaireId()))
									projectMap.put("40|" + p.getOpenaireId(), new ArrayList<>());
								projectMap.get("40|" + p.getOpenaireId()).add(community.getId());
							});
						}
					} catch (IOException e) {
						throw new RuntimeException(e);
					}
				} while (!cm.getLast());
			});
		return projectMap;
	}
}
