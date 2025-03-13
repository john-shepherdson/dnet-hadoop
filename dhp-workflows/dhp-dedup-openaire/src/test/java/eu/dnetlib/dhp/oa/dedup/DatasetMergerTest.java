
package eu.dnetlib.dhp.oa.dedup;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.io.Serializable;
import java.lang.reflect.InvocationTargetException;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;

import org.codehaus.jackson.map.ObjectMapper;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import eu.dnetlib.dhp.schema.oaf.DataInfo;
import eu.dnetlib.dhp.schema.oaf.Dataset;
import eu.dnetlib.dhp.schema.oaf.utils.MergeUtils;
import eu.dnetlib.pace.util.MapDocumentUtil;
import scala.Tuple2;

class DatasetMergerTest implements Serializable {

	private List<Tuple2<String, Dataset>> datasets;

	private String testEntityBasePath;
	private DataInfo dataInfo;
	private final String dedupId = "50|doi_________::3d18564ef27ebe9ef3bd8b4dec67e148";
	private Dataset dataset_top;

	@BeforeEach
	public void setUp() throws Exception {
		testEntityBasePath = Paths
			.get(SparkDedupTest.class.getResource("/eu/dnetlib/dhp/dedup/json").toURI())
			.toFile()
			.getAbsolutePath();

		datasets = readSample(testEntityBasePath + "/dataset_merge.json", Dataset.class);

		dataset_top = getTopPub(datasets);

		dataInfo = setDI();
	}

	@Test
	void datasetMergerTest() {
		Dataset pub_merged = MergeUtils.mergeGroup(datasets.stream().map(Tuple2::_2).iterator());

		// verify id
		assertEquals(dedupId, pub_merged.getId());
		assertEquals(2, pub_merged.getInstance().size());
	}

	public DataInfo setDI() {
		DataInfo dataInfo = new DataInfo();
		dataInfo.setTrust("0.9");
		dataInfo.setDeletedbyinference(false);
		dataInfo.setInferenceprovenance("testing");
		dataInfo.setInferred(true);
		return dataInfo;
	}

	public Dataset getTopPub(List<Tuple2<String, Dataset>> publications) {

		Double maxTrust = 0.0;
		Dataset maxPub = new Dataset();
		for (Tuple2<String, Dataset> publication : publications) {
			Double pubTrust = Double.parseDouble(publication._2().getDataInfo().getTrust());
			if (pubTrust > maxTrust) {
				maxTrust = pubTrust;
				maxPub = publication._2();
			}
		}
		return maxPub;
	}

	public <T> List<Tuple2<String, T>> readSample(String path, Class<T> clazz) {
		List<Tuple2<String, T>> res = new ArrayList<>();
		BufferedReader reader;
		try {
			reader = new BufferedReader(new FileReader(path));
			String line = reader.readLine();
			while (line != null) {
				res
					.add(
						new Tuple2<>(
							MapDocumentUtil.getJPathString("$.id", line),
							new ObjectMapper().readValue(line, clazz)));
				// read next line
				line = reader.readLine();
			}
			reader.close();
		} catch (IOException e) {
			e.printStackTrace();
		}

		return res;
	}

}
