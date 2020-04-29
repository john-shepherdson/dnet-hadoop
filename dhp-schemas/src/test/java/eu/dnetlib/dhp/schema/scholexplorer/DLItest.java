
package eu.dnetlib.dhp.schema.scholexplorer;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;

import org.junit.jupiter.api.Test;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;

import eu.dnetlib.dhp.schema.oaf.Qualifier;
import eu.dnetlib.dhp.schema.oaf.StructuredProperty;

public class DLItest {

	@Test
	public void testMergePublication() throws JsonProcessingException {
		DLIPublication a1 = new DLIPublication();
		a1.setPid(Arrays.asList(createSP("123456", "pdb", "dnet:pid_types")));
		a1.setTitle(Collections.singletonList(createSP("Un Titolo", "title", "dnetTitle")));
		a1.setDlicollectedfrom(Arrays.asList(createCollectedFrom("znd", "Zenodo", "complete")));
		a1.setCompletionStatus("complete");

		DLIPublication a = new DLIPublication();
		a
			.setPid(
				Arrays
					.asList(
						createSP("10.11", "doi", "dnet:pid_types"),
						createSP("123456", "pdb", "dnet:pid_types")));
		a.setTitle(Collections.singletonList(createSP("A Title", "title", "dnetTitle")));
		a
			.setDlicollectedfrom(
				Arrays
					.asList(
						createCollectedFrom("dct", "datacite", "complete"),
						createCollectedFrom("dct", "datacite", "incomplete")));
		a.setCompletionStatus("incomplete");

		a.mergeFrom(a1);

		ObjectMapper mapper = new ObjectMapper();
		System.out.println(mapper.writeValueAsString(a));
	}

	@Test
	public void testDeserialization() throws IOException {

		final String json = "{\"dataInfo\":{\"invisible\":false,\"inferred\":null,\"deletedbyinference\":false,\"trust\":\"0.9\",\"inferenceprovenance\":null,\"provenanceaction\":null},\"lastupdatetimestamp\":null,\"id\":\"60|bd9352547098929a394655ad1a44a479\",\"originalId\":[\"bd9352547098929a394655ad1a44a479\"],\"collectedfrom\":[{\"key\":\"dli_________::datacite\",\"value\":\"Datasets in Datacite\",\"dataInfo\":null,\"blank\":false}],\"pid\":[{\"value\":\"10.7925/DRS1.DUCHAS_5078760\",\"qualifier\":{\"classid\":\"doi\",\"classname\":\"doi\",\"schemeid\":\"dnet:pid_types\",\"schemename\":\"dnet:pid_types\",\"blank\":false},\"dataInfo\":null}],\"dateofcollection\":\"2020-01-09T08:29:31.885Z\",\"dateoftransformation\":null,\"extraInfo\":null,\"oaiprovenance\":null,\"author\":[{\"fullname\":\"Cathail, S. Ó\",\"name\":null,\"surname\":null,\"rank\":null,\"pid\":null,\"affiliation\":null},{\"fullname\":\"Donnell, Breda Mc\",\"name\":null,\"surname\":null,\"rank\":null,\"pid\":null,\"affiliation\":null},{\"fullname\":\"Ireland. Department of Arts, Culture, and the Gaeltacht\",\"name\":null,\"surname\":null,\"rank\":null,\"pid\":null,\"affiliation\":null},{\"fullname\":\"University College Dublin\",\"name\":null,\"surname\":null,\"rank\":null,\"pid\":null,\"affiliation\":null},{\"fullname\":\"National Folklore Foundation\",\"name\":null,\"surname\":null,\"rank\":null,\"pid\":null,\"affiliation\":null},{\"fullname\":\"Cathail, S. Ó\",\"name\":null,\"surname\":null,\"rank\":null,\"pid\":null,\"affiliation\":null},{\"fullname\":\"Donnell, Breda Mc\",\"name\":null,\"surname\":null,\"rank\":null,\"pid\":null,\"affiliation\":null}],\"resulttype\":null,\"language\":null,\"country\":null,\"subject\":[{\"value\":\"Recreation\",\"qualifier\":{\"classid\":\"dnet:subject\",\"classname\":\"dnet:subject\",\"schemeid\":\"unknown\",\"schemename\":\"unknown\",\"blank\":false},\"dataInfo\":null},{\"value\":\"Entertainments and recreational activities\",\"qualifier\":{\"classid\":\"dnet:subject\",\"classname\":\"dnet:subject\",\"schemeid\":\"unknown\",\"schemename\":\"unknown\",\"blank\":false},\"dataInfo\":null},{\"value\":\"Siamsaíocht agus caitheamh aimsire\",\"qualifier\":{\"classid\":\"dnet:subject\",\"classname\":\"dnet:subject\",\"schemeid\":\"unknown\",\"schemename\":\"unknown\",\"blank\":false},\"dataInfo\":null}],\"title\":[{\"value\":\"Games We Play\",\"qualifier\":null,\"dataInfo\":null}],\"relevantdate\":[{\"value\":\"1938-09-28\",\"qualifier\":{\"classid\":\"date\",\"classname\":\"date\",\"schemeid\":\"dnet::date\",\"schemename\":\"dnet::date\",\"blank\":false},\"dataInfo\":null}],\"description\":[{\"value\":\"Story collected by Breda Mc Donnell, a student at Tenure school (Tinure, Co. Louth) (no informant identified).\",\"dataInfo\":null}],\"dateofacceptance\":null,\"publisher\":{\"value\":\"University College Dublin\",\"dataInfo\":null},\"embargoenddate\":null,\"source\":null,\"fulltext\":null,\"format\":null,\"contributor\":null,\"resourcetype\":null,\"coverage\":null,\"refereed\":null,\"context\":null,\"processingchargeamount\":null,\"processingchargecurrency\":null,\"externalReference\":null,\"instance\":[],\"storagedate\":null,\"device\":null,\"size\":null,\"version\":null,\"lastmetadataupdate\":null,\"metadataversionnumber\":null,\"geolocation\":null,\"dlicollectedfrom\":[{\"id\":\"dli_________::datacite\",\"name\":\"Datasets in Datacite\",\"completionStatus\":\"complete\",\"collectionMode\":\"resolved\"}],\"completionStatus\":\"complete\"}";

		ObjectMapper mapper = new ObjectMapper();
		mapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
		DLIDataset dliDataset = mapper.readValue(json, DLIDataset.class);
		mapper.enable(SerializationFeature.INDENT_OUTPUT);
		System.out.println(mapper.writeValueAsString(dliDataset));
	}

	private ProvenaceInfo createCollectedFrom(
		final String id, final String name, final String completionStatus) {
		ProvenaceInfo p = new ProvenaceInfo();
		p.setId(id);
		p.setName(name);
		p.setCompletionStatus(completionStatus);
		return p;
	}

	private StructuredProperty createSP(
		final String value, final String className, final String schemeName) {
		StructuredProperty p = new StructuredProperty();
		p.setValue(value);
		Qualifier schema = new Qualifier();
		schema.setClassname(className);
		schema.setClassid(className);
		schema.setSchemename(schemeName);
		schema.setSchemeid(schemeName);
		p.setQualifier(schema);
		return p;
	}
}
