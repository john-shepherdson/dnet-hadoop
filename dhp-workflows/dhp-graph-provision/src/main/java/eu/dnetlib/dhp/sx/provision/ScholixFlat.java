
package eu.dnetlib.dhp.sx.provision;

import java.util.ArrayList;
import java.util.List;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

import eu.dnetlib.dhp.schema.sx.scholix.Scholix;
import eu.dnetlib.dhp.schema.sx.scholix.ScholixResource;

public class ScholixFlat {
	private static ObjectMapper MAPPER = new ObjectMapper();
	private List<String> linkProvider = new ArrayList<>();

	private String publicationDate;

	private List<String> sourceLinkPublisher = new ArrayList<>();
	private List<String> targetLinkPublisher = new ArrayList<>();

	private String sourceDnetIdentifier;
	private String targetDnetIdentifier;
	private List<String> sourcePids = new ArrayList<>();
	private List<String> sourcePidTypes = new ArrayList<>();
	private List<String> targetPids = new ArrayList<>();
	private List<String> targetPidTypes = new ArrayList<>();

	private String json;

	public void addLinkProvider(final String providerName) {
		addStringToList(providerName, this.linkProvider);
	}

	public void addSourceLinkPublisher(final String linkPublisher) {
		addStringToList(linkPublisher, sourceLinkPublisher);

	}

	public void addTargetLinkPublisher(final String linkPublisher) {
		addStringToList(linkPublisher, targetLinkPublisher);

	}

	public void addSourcePid(final String pid) {
		addStringToList(pid, sourcePids);
	}

	public void addSourcePidType(final String pidType) {
		addStringToList(pidType, sourcePidTypes);
	}

	public void addTargetPidType(final String pidType) {
		addStringToList(pidType, targetPidTypes);
	}

	public void addTargetPid(final String pid) {
		addStringToList(pid, targetPids);
	}

	public void addStringToList(final String s, final List<String> l) {
		if (l != null && !l.contains(s))
			l.add(s);
	}

	public String getSourceDnetIdentifier() {
		return sourceDnetIdentifier;
	}

	public void setSourceDnetIdentifier(String sourceDnetIdentifier) {
		this.sourceDnetIdentifier = sourceDnetIdentifier;
	}

	public String getTargetDnetIdentifier() {
		return targetDnetIdentifier;
	}

	public void setTargetDnetIdentifier(String targetDnetIdentifier) {
		this.targetDnetIdentifier = targetDnetIdentifier;
	}

	public List<String> getSourcePids() {
		return sourcePids;
	}

	public void setSourcePids(List<String> sourcePids) {
		this.sourcePids = sourcePids;
	}

	public List<String> getSourcePidTypes() {
		return sourcePidTypes;
	}

	public void setSourcePidTypes(List<String> sourcePidTypes) {
		this.sourcePidTypes = sourcePidTypes;
	}

	public List<String> getTargetPids() {
		return targetPids;
	}

	public void setTargetPids(List<String> targetPids) {
		this.targetPids = targetPids;
	}

	public List<String> getTargetPidTypes() {
		return targetPidTypes;
	}

	public void setTargetPidTypes(List<String> targetPidTypes) {
		this.targetPidTypes = targetPidTypes;
	}

	public List<String> getSourceLinkPublisher() {
		return sourceLinkPublisher;
	}

	public void setSourceLinkPublisher(List<String> sourceLinkPublisher) {
		this.sourceLinkPublisher = sourceLinkPublisher;
	}

	public List<String> getTargetLinkPublisher() {
		return targetLinkPublisher;
	}

	public void setTargetLinkPublisher(List<String> targetLinkPublisher) {
		this.targetLinkPublisher = targetLinkPublisher;
	}

	public List<String> getLinkProvider() {
		return linkProvider;
	}

	public void setLinkProvider(List<String> linkProvider) {
		this.linkProvider = linkProvider;
	}

	public String getPublicationDate() {
		return publicationDate;
	}

	public void setPublicationDate(String publicationDate) {
		this.publicationDate = publicationDate;
	}

	public String getJson() {
		return json;
	}

	public void setJson(String json) {
		this.json = json;
	}

	public static ScholixFlat fromScholix(final Scholix scholix) throws JsonProcessingException {
		if (scholix == null || scholix.getSource() == null || scholix.getTarget() == null)
			return null;
		final ScholixFlat flat = new ScholixFlat();
		if (scholix.getLinkprovider() != null)
			scholix.getLinkprovider().forEach(l -> flat.addLinkProvider(l.getName()));

		flat.setPublicationDate(scholix.getPublicationDate());

		final ScholixResource source = scholix.getSource();
		flat.setSourceDnetIdentifier(source.getDnetIdentifier());
		if (source.getIdentifier() != null) {
			source.getIdentifier().forEach(i -> {
				flat.addSourcePid(i.getIdentifier());
				flat.addSourcePidType(i.getSchema());
			});
		}
		if (source.getPublisher() != null) {
			source.getPublisher().forEach(p -> flat.addSourceLinkPublisher(p.getName()));
		}

		final ScholixResource target = scholix.getSource();
		flat.setTargetDnetIdentifier(target.getDnetIdentifier());
		if (target.getIdentifier() != null) {
			target.getIdentifier().forEach(i -> {
				flat.addTargetPid(i.getIdentifier());
				flat.addTargetPidType(i.getSchema());
			});
		}
		if (target.getPublisher() != null) {
			target.getPublisher().forEach(p -> flat.addTargetLinkPublisher(p.getName()));
		}
		flat.setJson(MAPPER.writeValueAsString(scholix));
		return flat;
	}

}
