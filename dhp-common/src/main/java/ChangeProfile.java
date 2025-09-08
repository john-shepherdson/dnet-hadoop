import java.io.*;
import java.nio.file.*;
import java.util.*;
import java.util.stream.Collectors;

import javax.xml.parsers.*;
import javax.xml.transform.*;
import javax.xml.transform.dom.DOMSource;
import javax.xml.transform.stream.StreamResult;

import org.w3c.dom.*;

public class ChangeProfile {
	static Set<String> activeProfiles = new HashSet<>();
	static Set<String> inactiveProfiles = new HashSet<>();

	public static void main(String[] args) throws Exception {
		String activate = null;
		String deactivate = null;
		String suffix = null;

		for (int i = 0; i < args.length; i++) {
			switch (args[i]) {
				case "--activate":
					activate = args[++i];
					activeProfiles.addAll(Arrays.asList(activate.split(",")));
					break;
				case "--deactivate":
					deactivate = args[++i];
					inactiveProfiles.addAll(Arrays.asList(deactivate.split(",")));
					break;
				case "--suffix":
					suffix = args[++i];
					break;
			}
		}

		if (activate == null) {
			System.err.println("Missing required argument: --activate");
			System.exit(1);
		}

		if (suffix == null) {
			suffix = "";
		}

		List<Path> poms = findPomFiles(Paths.get("."));
		Collections.sort(poms);

		Map<String, String> artifactIdMap = new HashMap<>();

		// First pass: collect transformed artifactIds
		for (Path pom : poms) {
			Document doc = loadXml(pom);
			String artifactId = getArtifactId(doc);
			String newId = transformArtifactId(artifactId, suffix);
			artifactIdMap.put(artifactId, newId);
		}

		// Rewrite each pom.xml
		for (Path pom : poms) {
			System.out.println("🔧 Updating: " + pom);
			rewritePom(pom, artifactIdMap);
		}

		System.out.println("✅ Done updating all pom.xml files.");
	}

	static String transformArtifactId(String id, String suffix) {
		int lastUnderscore = id.lastIndexOf('_');
		if (lastUnderscore > 0) {
			id = id.substring(0, lastUnderscore);
		}

		if (suffix.isEmpty())
			return id;

		return id + "_" + suffix;
	}

	static List<Path> findPomFiles(Path dir) throws IOException {
		try (java.util.stream.Stream<Path> files = Files.walk(dir)) {
			return files
				.filter(p -> p.getFileName().toString().equals("pom.xml"))
				.collect(Collectors.toList());
		}
	}

	static Document loadXml(Path path) throws Exception {
		DocumentBuilderFactory dbf = DocumentBuilderFactory.newInstance();
		dbf.setNamespaceAware(true);
		DocumentBuilder db = dbf.newDocumentBuilder();
		return db.parse(path.toFile());
	}

	static String getArtifactId(Document doc) {
		NodeList list = doc.getElementsByTagName("project").item(0).getChildNodes();

		for (int j = 0; j < list.getLength(); j++) {
			Node pnode = list.item(j);
			if (pnode.getNodeName().equals("artifactId")) {
				return pnode.getTextContent().trim();
			}
		}

		return "";
	}

	static String getText(Element doc, String tagName) {
		NodeList list = doc.getElementsByTagName(tagName);
		if (list.getLength() > 0) {
			return list.item(0).getTextContent().trim();
		}
		return null;
	}

	static void rewritePom(Path pomPath, Map<String, String> artifactIdMap) throws Exception {
		Document doc = loadXml(pomPath);

		// Update project.artifactId and project.parent.artifactId
		NodeList aidNodes = doc.getElementsByTagName("project").item(0).getChildNodes();
		for (int i = 0; i < aidNodes.getLength(); i++) {
			Node node = aidNodes.item(i);

			if (node.getNodeName().equals("parent")) {
				NodeList pList = node.getChildNodes();
				for (int j = 0; j < pList.getLength(); j++) {
					Node pnode = pList.item(j);
					if (pnode.getNodeName().equals("artifactId")) {
						node = pnode;
						break;
					}
				}
			}

			if (!node.getNodeName().equals("artifactId")) {
				continue;

			}
			String original = node.getTextContent().trim();
			if (artifactIdMap.containsKey(original)) {
				node.setTextContent(artifactIdMap.get(original));
			}
		}

		// Update profiles
		NodeList profiles = doc.getElementsByTagName("profile");
		for (int i = 0; i < profiles.getLength(); i++) {
			Element profile = (Element) profiles.item(i);
			String profileId = profile.getElementsByTagName("id").item(0).getTextContent().trim();

			if (activeProfiles.contains(profileId)) {
				Element activation = getOrCreateChild(profile, "activation");
				Element active = getOrCreateChild(activation, "activeByDefault");
				active.setTextContent("true");
			} else if (inactiveProfiles.contains(profileId)) {
				Element activation = getChild(profile, "activation");
				if (activation != null) {
					Element active = getOrCreateChild(activation, "activeByDefault");
					if (active != null) {
						active.setTextContent("false");
					}
				}
			}
		}

		// Promote properties from activated profile to <project><properties>
		profiles = doc.getElementsByTagName("profile");
		Element projectProperties = getOrCreateChild((Element) doc.getDocumentElement(), "properties");

		for (int i = 0; i < profiles.getLength(); i++) {
			Element profile = (Element) profiles.item(i);
			String profileId = getText(profile, "id");

			if (!activeProfiles.contains(profileId))
				continue;

			NodeList children = profile.getElementsByTagName("properties");
			if (children.getLength() == 0)
				continue;

			Element profileProps = (Element) children.item(0);
			NodeList props = profileProps.getChildNodes();

			for (int j = 0; j < props.getLength(); j++) {
				Node prop = props.item(j);
				if (prop.getNodeType() != Node.ELEMENT_NODE)
					continue;

				String name = prop.getNodeName();
				String value = prop.getTextContent();

				// Remove if already exists
				Node existing = null;
				NodeList topProps = projectProperties.getElementsByTagName(name);
				for (int k = 0; k < topProps.getLength(); k++) {
					if (topProps.item(k).getNodeName().equals(name)) {
						existing = topProps.item(k);
						break;
					}
				}
				if (existing != null) {
					projectProperties.removeChild(existing);
				}

				// Promote new one
				Element newProp = doc.createElement(name);
				newProp.setTextContent(value);
				projectProperties.appendChild(newProp);
			}

			System.out.println("⬆️  Promoted properties from profile: " + profileId);
		}
		// Save updated file
		Transformer transformer = TransformerFactory.newInstance().newTransformer();
		DOMSource src = new DOMSource(doc);
		StreamResult out = new StreamResult(pomPath.toFile());
		transformer.transform(src, out);
	}

	static Element getChild(Element parent, String tag) {
		NodeList children = parent.getElementsByTagName(tag);
		if (children.getLength() > 0) {
			return (Element) children.item(0);
		}
		return null;
	}

	static Element getOrCreateChild(Element parent, String tag) {
		NodeList children = parent.getElementsByTagName(tag);
		if (children.getLength() > 0) {
			return (Element) children.item(0);
		} else {
			Element newChild = parent.getOwnerDocument().createElement(tag);
			parent.appendChild(newChild);
			return newChild;
		}
	}
}
