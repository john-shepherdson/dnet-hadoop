
package eu.dnetlib.dhp.oa.dedup.graph;

import java.io.IOException;
import java.io.Serializable;
import java.util.Set;
import java.util.stream.Collectors;

import eu.dnetlib.dhp.utils.DHPUtils;
import org.apache.commons.lang.StringUtils;
import org.codehaus.jackson.annotate.JsonIgnore;

import com.fasterxml.jackson.databind.ObjectMapper;

import eu.dnetlib.dhp.oa.dedup.DedupUtility;
import eu.dnetlib.pace.util.PaceException;

public class ConnectedComponent implements Serializable {

	private Set<String> docIds;
	private String ccId;

	public ConnectedComponent(Set<String> docIds, final int cut) {
		this.docIds = docIds;
		createID();
		if (cut > 0 && docIds.size() > cut) {
			this.docIds = docIds
				.stream()
				.filter(s -> !ccId.equalsIgnoreCase(s))
				.limit(cut - 1)
				.collect(Collectors.toSet());
			this.docIds.add(ccId);
		}
	}

	public String createID() {
		if (docIds.size() > 1) {
			final String s = getMin();
			String prefix = s.split("\\|")[0];
			ccId = prefix + "|dedup_wf_001::" + DHPUtils.md5(s);
			return ccId;
		} else {
			return docIds.iterator().next();
		}
	}

	@JsonIgnore
	public String getMin() {

		final StringBuilder min = new StringBuilder();

		docIds
			.forEach(
				i -> {
					if (StringUtils.isBlank(min.toString())) {
						min.append(i);
					} else {
						if (min.toString().compareTo(i) > 0) {
							min.setLength(0);
							min.append(i);
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

	public Set<String> getDocIds() {
		return docIds;
	}

	public void setDocIds(Set<String> docIds) {
		this.docIds = docIds;
	}

	public String getCcId() {
		return ccId;
	}

	public void setCcId(String ccId) {
		this.ccId = ccId;
	}
}
