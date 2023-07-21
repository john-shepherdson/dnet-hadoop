
package eu.dnetlib.dhp.actionmanager.bipmodel.score.deserializers;

import static eu.dnetlib.dhp.actionmanager.Constants.*;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import com.opencsv.bean.CsvBindByPosition;

import eu.dnetlib.dhp.schema.common.ModelConstants;
import eu.dnetlib.dhp.schema.oaf.KeyValue;
import eu.dnetlib.dhp.schema.oaf.Measure;
import eu.dnetlib.dhp.schema.oaf.utils.OafMapperUtils;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

@NoArgsConstructor
@AllArgsConstructor
@Getter
@Setter
public class BipProjectModel {
	String projectId;

	String numOfInfluentialResults;

	String numOfPopularResults;

	String totalImpulse;

	String totalCitationCount;

	// each project bip measure has exactly one value, hence one key-value pair
	private Measure createMeasure(String measureId, String measureValue) {

		KeyValue kv = new KeyValue();
		kv.setKey("score");
		kv.setValue(measureValue);
		kv
			.setDataInfo(
				OafMapperUtils
					.dataInfo(
						false,
						UPDATE_DATA_INFO_TYPE,
						true,
						false,
						OafMapperUtils
							.qualifier(
								UPDATE_MEASURE_BIP_CLASS_ID,
								UPDATE_CLASS_NAME,
								ModelConstants.DNET_PROVENANCE_ACTIONS,
								ModelConstants.DNET_PROVENANCE_ACTIONS),
						""));

		Measure measure = new Measure();
		measure.setId(measureId);
		measure.setUnit(Collections.singletonList(kv));
		return measure;
	}

	public List<Measure> toMeasures() {
		return Arrays
			.asList(
				createMeasure("numOfInfluentialResults", numOfInfluentialResults),
				createMeasure("numOfPopularResults", numOfPopularResults),
				createMeasure("totalImpulse", totalImpulse),
				createMeasure("totalCitationCount", totalCitationCount));
	}

}
