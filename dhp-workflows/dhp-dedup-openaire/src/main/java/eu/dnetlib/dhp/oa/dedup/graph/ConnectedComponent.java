
package eu.dnetlib.dhp.oa.dedup.graph;

import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import org.apache.commons.lang3.StringUtils;
import org.codehaus.jackson.annotate.JsonIgnore;

import com.fasterxml.jackson.databind.ObjectMapper;

import eu.dnetlib.dhp.utils.DHPUtils;
import eu.dnetlib.pace.util.PaceException;

public class ConnectedComponent implements Serializable {

	private String ccId = "";
	private List<String> ids = Collections.EMPTY_LIST;

	private static final String CONNECTED_COMPONENT_ID_PREFIX = "connect_comp";

	public ConnectedComponent() {
	}

	public ConnectedComponent(Set<String> ids, final int cut) {
		this.ids = new ArrayList<>(ids);
		this.ccId = createDefaultID();

		if (cut > 0 && ids.size() > cut) {
			this.ids = ids
				.stream()
				.filter(id -> !ccId.equalsIgnoreCase(id))
				.limit(cut - 1)
				.distinct()
				.collect(Collectors.toList());
//			this.ids.add(ccId); ??
		}
	}

	public ConnectedComponent(String ccId, Set<String> ids) {
		this.ccId = ccId;
		this.ids = new ArrayList<>(ids);
	}

	public String createDefaultID() {
		if (ids.size() > 1) {
			final String s = getMin();
			String prefix = s.split("\\|")[0];
			ccId = prefix + "|" + CONNECTED_COMPONENT_ID_PREFIX + "::" + DHPUtils.md5(s);
			return ccId;
		} else {
			return ids.iterator().next();
		}
	}

	@JsonIgnore
	public String getMin() {

		final StringBuilder min = new StringBuilder();

		ids
			.forEach(
				id -> {
					if (StringUtils.isBlank(min.toString())) {
						min.append(id);
					} else {
						if (min.toString().compareTo(id) > 0) {
							min.setLength(0);
							min.append(id);
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

	public List<String> getIds() {
		return ids;
	}

	public void setIds(List<String> ids) {
		this.ids =ids;
	}

	public String getCcId() {
		return ccId;
	}

	public void setCcId(String ccId) {
		this.ccId = ccId;
	}
}
