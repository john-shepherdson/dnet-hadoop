
package eu.dnetlib.dhp.schema.scholexplorer;

import java.io.Serializable;
import java.util.*;
import java.util.stream.Collectors;

import org.apache.commons.lang3.StringUtils;

import eu.dnetlib.dhp.schema.oaf.Oaf;
import eu.dnetlib.dhp.schema.oaf.StructuredProperty;

public class DLIUnknown extends Oaf implements Serializable {

	private String id;

	private List<StructuredProperty> pid;

	private String dateofcollection;

	private String dateoftransformation;

	private List<ProvenaceInfo> dlicollectedfrom;

	private String completionStatus = "incomplete";

	public String getCompletionStatus() {
		return completionStatus;
	}

	public void setCompletionStatus(String completionStatus) {
		this.completionStatus = completionStatus;
	}

	public List<ProvenaceInfo> getDlicollectedfrom() {
		return dlicollectedfrom;
	}

	public void setDlicollectedfrom(List<ProvenaceInfo> dlicollectedfrom) {
		this.dlicollectedfrom = dlicollectedfrom;
	}

	public String getId() {
		return id;
	}

	public void setId(String id) {
		this.id = id;
	}

	public List<StructuredProperty> getPid() {
		return pid;
	}

	public void setPid(List<StructuredProperty> pid) {
		this.pid = pid;
	}

	public String getDateofcollection() {
		return dateofcollection;
	}

	public void setDateofcollection(String dateofcollection) {
		this.dateofcollection = dateofcollection;
	}

	public String getDateoftransformation() {
		return dateoftransformation;
	}

	public void setDateoftransformation(String dateoftransformation) {
		this.dateoftransformation = dateoftransformation;
	}

	public void mergeFrom(DLIUnknown p) {
		if ("complete".equalsIgnoreCase(p.completionStatus))
			completionStatus = "complete";
		dlicollectedfrom = mergeProvenance(dlicollectedfrom, p.getDlicollectedfrom());
		if (StringUtils.isEmpty(id) && StringUtils.isNoneEmpty(p.getId()))
			id = p.getId();
		if (StringUtils.isEmpty(dateofcollection) && StringUtils.isNoneEmpty(p.getDateofcollection()))
			dateofcollection = p.getDateofcollection();

		if (StringUtils.isEmpty(dateoftransformation) && StringUtils.isNoneEmpty(p.getDateoftransformation()))
			dateofcollection = p.getDateoftransformation();
		pid = mergeLists(pid, p.getPid());
	}

	protected <T> List<T> mergeLists(final List<T>... lists) {

		return Arrays
			.stream(lists)
			.filter(Objects::nonNull)
			.flatMap(List::stream)
			.filter(Objects::nonNull)
			.distinct()
			.collect(Collectors.toList());
	}

	private List<ProvenaceInfo> mergeProvenance(
		final List<ProvenaceInfo> a, final List<ProvenaceInfo> b) {
		Map<String, ProvenaceInfo> result = new HashMap<>();
		if (a != null)
			a
				.forEach(
					p -> {
						if (p != null && StringUtils.isNotBlank(p.getId()) && result.containsKey(p.getId())) {
							if ("incomplete".equalsIgnoreCase(result.get(p.getId()).getCompletionStatus())
								&& StringUtils.isNotBlank(p.getCompletionStatus())) {
								result.put(p.getId(), p);
							}

						} else if (p != null && p.getId() != null && !result.containsKey(p.getId()))
							result.put(p.getId(), p);
					});
		if (b != null)
			b
				.forEach(
					p -> {
						if (p != null && StringUtils.isNotBlank(p.getId()) && result.containsKey(p.getId())) {
							if ("incomplete".equalsIgnoreCase(result.get(p.getId()).getCompletionStatus())
								&& StringUtils.isNotBlank(p.getCompletionStatus())) {
								result.put(p.getId(), p);
							}

						} else if (p != null && p.getId() != null && !result.containsKey(p.getId()))
							result.put(p.getId(), p);
					});

		return new ArrayList<>(result.values());
	}
}
