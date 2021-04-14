
package eu.dnetlib.dhp.oa.dedup.graph;

import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.google.common.collect.Lists;
import eu.dnetlib.dhp.oa.dedup.IdGenerator;
import eu.dnetlib.dhp.oa.dedup.model.Identifier;
import eu.dnetlib.dhp.schema.common.EntityType;
import eu.dnetlib.dhp.schema.common.ModelSupport;
import eu.dnetlib.dhp.schema.oaf.OafEntity;
import eu.dnetlib.pace.config.DedupConfig;
import eu.dnetlib.pace.util.MapDocumentUtil;
import org.apache.commons.lang.StringUtils;
import org.apache.spark.api.java.function.MapFunction;
import org.codehaus.jackson.annotate.JsonIgnore;

import com.fasterxml.jackson.databind.ObjectMapper;

import eu.dnetlib.dhp.oa.dedup.DedupUtility;
import eu.dnetlib.dhp.utils.DHPUtils;
import eu.dnetlib.pace.util.PaceException;

public class ConnectedComponent implements Serializable {

	private String ccId;
	private Set<String> entities;

	protected static final ObjectMapper OBJECT_MAPPER = new ObjectMapper()
			.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);

	public <T extends OafEntity> ConnectedComponent(Set<String> entities, String subEntity, final int cut) {
		this.entities = entities;
		final Class<T> clazz = ModelSupport.entityTypes.get(EntityType.valueOf(subEntity));

		List<Identifier<T>> identifiers = Lists.newArrayList();

		entities.forEach(e -> {
			try {
				T entity = OBJECT_MAPPER.readValue(e, clazz);
				identifiers.add(Identifier.newInstance(entity));
			} catch (IOException e1) {
			}
		});

		this.ccId = IdGenerator.generate(
				identifiers,
				createDefaultID()
		);

		if (cut > 0 && entities.size() > cut) {
			this.entities = entities
					.stream()
					.filter(e -> !ccId.equalsIgnoreCase(MapDocumentUtil.getJPathString("$.id", e)))
					.limit(cut - 1)
					.collect(Collectors.toSet());
		}
	}

	public String createDefaultID() {
		if (entities.size() > 1) {
			final String s = getMin();
			String prefix = s.split("\\|")[0];
			ccId = prefix + "|dedup_wf_001::" + DHPUtils.md5(s);
			return ccId;
		} else {
			return MapDocumentUtil.getJPathString("$.id", entities.iterator().next());
		}
	}

	@JsonIgnore
	public String getMin() {

		final StringBuilder min = new StringBuilder();

		entities
			.forEach(
				e -> {
					if (StringUtils.isBlank(min.toString())) {
						min.append(MapDocumentUtil.getJPathString("$.id", e));
					} else {
						if (min.toString().compareTo(MapDocumentUtil.getJPathString("$.id", e)) > 0) {
							min.setLength(0);
							min.append(MapDocumentUtil.getJPathString("$.id", e));
						}
					}
				});
		return min.toString();
	}

	@Override
	public String toString() {
		ObjectMapper mapper = new ObjectMapper();
		try {
			return mapper.writeValueAsString(this);
		} catch (IOException e) {
			throw new PaceException("Failed to create Json: ", e);
		}
	}

	public Set<String> getEntities() {
		return entities;
	}

	public void setEntities(Set<String> docIds) {
		this.entities = entities;
	}

	public String getCcId() {
		return ccId;
	}

	public void setCcId(String ccId) {
		this.ccId = ccId;
	}
}
