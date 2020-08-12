package eu.dnetlib.dhp.common.api;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class ZenodoAPIClientTest {

	private final String URL_STRING = "https://sandbox.zenodo.org/api/deposit/depositions";
	private final String ACCESS_TOKEN = "";

	private final String CONCEPT_REC_ID = "657113";

	@Test
	public void testNewDeposition() throws IOException {

		ZenodoAPIClient client = new ZenodoAPIClient(URL_STRING,
			ACCESS_TOKEN);
		Assertions.assertEquals(201, client.newDeposition());

		File file = new File(getClass()
			.getResource("/eu/dnetlib/dhp/common/api/COVID-19.json.gz")
			.getPath());

		InputStream is = new FileInputStream(file);

		Assertions.assertEquals(200, client.uploadIS(is, "COVID-19.json.gz", file.length()));

		String metadata = "{\"metadata\":{\"access_right\":\"open\",\"communities\":[{\"identifier\":\"openaire-research-graph\"}],\"creators\":[{\"affiliation\":\"ISTI - CNR\",\"name\":\"Bardi, Alessia\",\"orcid\":\"0000-0002-1112-1292\"},{\"affiliation\":\"eifl\", \"name\":\"Kuchma, Iryna\"},{\"affiliation\":\"BIH\", \"name\":\"Brobov, Evgeny\"},{\"affiliation\":\"GIDIF RBM\", \"name\":\"Truccolo, Ivana\"},{\"affiliation\":\"unesp\", \"name\":\"Monteiro, Elizabete\"},{\"affiliation\":\"und\", \"name\":\"Casalegno, Carlotta\"},{\"affiliation\":\"CARL ABRC\", \"name\":\"Clary, Erin\"},{\"affiliation\":\"The University of Edimburgh\", \"name\":\"Romanowski, Andrew\"},{\"affiliation\":\"ISTI - CNR\", \"name\":\"Pavone, Gina\"},{\"affiliation\":\"ISTI - CNR\", \"name\":\"Artini, Michele\"},{\"affiliation\":\"ISTI - CNR\",\"name\":\"Atzori, Claudio\",\"orcid\":\"0000-0001-9613-6639\"},{\"affiliation\":\"University of Bielefeld\",\"name\":\"Bäcker, Amelie\",\"orcid\":\"0000-0001-6015-2063\"},{\"affiliation\":\"ISTI - CNR\",\"name\":\"Baglioni, Miriam\",\"orcid\":\"0000-0002-2273-9004\"},{\"affiliation\":\"University of Bielefeld\",\"name\":\"Czerniak, Andreas\",\"orcid\":\"0000-0003-3883-4169\"},{\"affiliation\":\"ISTI - CNR\",\"name\":\"De Bonis, Michele\"},{\"affiliation\":\"Athena Research and Innovation Centre\",\"name\":\"Dimitropoulos, Harry\"},{\"affiliation\":\"Athena Research and Innovation Centre\",\"name\":\"Foufoulas, Ioannis\"},{\"affiliation\":\"University of Warsaw\",\"name\":\"Horst, Marek\"},{\"affiliation\":\"Athena Research and Innovation Centre\",\"name\":\"Iatropoulou, Katerina\"},{\"affiliation\":\"University of Warsaw\",\"name\":\"Jacewicz, Przemyslaw\"},{\"affiliation\":\"Athena Research and Innovation Centre\",\"name\":\"Kokogiannaki, Argiro\", \"orcid\":\"0000-0002-3880-0244\"},{\"affiliation\":\"ISTI - CNR\",\"name\":\"La Bruzzo, Sandro\",\"orcid\":\"0000-0003-2855-1245\"},{\"affiliation\":\"ISTI - CNR\",\"name\":\"Lazzeri, Emma\"},{\"affiliation\":\"University of Bielefeld\",\"name\":\"Löhden, Aenne\"},{\"affiliation\":\"ISTI - CNR\",\"name\":\"Manghi, Paolo\",\"orcid\":\"0000-0001-7291-3210\"},{\"affiliation\":\"ISTI - CNR\",\"name\":\"Mannocci, Andrea\",\"orcid\":\"0000-0002-5193-7851\"},{\"affiliation\":\"Athena Research and Innovation Center\",\"name\":\"Manola, Natalia\"},{\"affiliation\":\"ISTI - CNR\",\"name\":\"Ottonello, Enrico\"},{\"affiliation\":\"University of Bielefeld\",\"name\":\"Shirrwagen, Jochen\"}],\"description\":\"\\u003cp\\u003eThis dump provides access to the metadata records of publications, research data, software and projects that may be relevant to the Corona Virus Disease (COVID-19) fight. The dump contains records of the OpenAIRE COVID-19 Gateway (https://covid-19.openaire.eu/), identified via full-text mining and inference techniques applied to the OpenAIRE Research Graph (https://explore.openaire.eu/). The Graph is one of the largest Open Access collections of metadata records and links between publications, datasets, software, projects, funders, and organizations, aggregating 12,000+ scientific data sources world-wide, among which the Covid-19 data sources Zenodo COVID-19 Community, WHO (World Health Organization), BIP! FInder for COVID-19, Protein Data Bank, Dimensions, scienceOpen, and RSNA. \\u003cp\\u003eThe dump consists of a gzip file containing one json per line. Each json is compliant to the schema available at https://doi.org/10.5281/zenodo.3974226\\u003c/p\\u003e \",\"title\":\"OpenAIRE Covid-19 publications, datasets, software and projects metadata.\",\"upload_type\":\"dataset\",\"version\":\"1.0\"}}";

		Assertions.assertEquals(200, client.sendMretadata(metadata));

		Assertions.assertEquals(202, client.publish());

	}

	@Test
	public void testNewVersionNewName() throws IOException, MissingConceptDoiException {

		ZenodoAPIClient client = new ZenodoAPIClient(URL_STRING,
			ACCESS_TOKEN);

		Assertions.assertEquals(201, client.newVersion(CONCEPT_REC_ID));

		File file = new File(getClass()
			.getResource("/eu/dnetlib/dhp/common/api/newVersion")
			.getPath());

		InputStream is = new FileInputStream(file);

		Assertions.assertEquals(200, client.uploadIS(is, "newVersion_deposition", file.length()));

		Assertions.assertEquals(202, client.publish());

	}

	@Test
	public void testNewVersionOldName() throws IOException, MissingConceptDoiException {

		ZenodoAPIClient client = new ZenodoAPIClient(URL_STRING,
			ACCESS_TOKEN);

		Assertions.assertEquals(201, client.newVersion(CONCEPT_REC_ID));

		File file = new File(getClass()
			.getResource("/eu/dnetlib/dhp/common/api/newVersion2")
			.getPath());

		InputStream is = new FileInputStream(file);

		Assertions.assertEquals(200, client.uploadIS(is, "newVersion_deposition", file.length()));

		Assertions.assertEquals(202, client.publish());

	}

}
