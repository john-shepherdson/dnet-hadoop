
package eu.dnetlib.dhp.actionmanager.project.csvutils;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;

import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVRecord;
import org.apache.commons.lang.reflect.FieldUtils;

public class CSVParser {

	public <R> List<R> parse(String csvFile, String classForName)
		throws ClassNotFoundException, IOException, IllegalAccessException, InstantiationException {
		final CSVFormat format = CSVFormat.EXCEL
			.withHeader()
			.withDelimiter(';')
			.withQuote('"')
			.withTrim();
		List<R> ret = new ArrayList<>();
		final org.apache.commons.csv.CSVParser parser = org.apache.commons.csv.CSVParser.parse(csvFile, format);
		final Set<String> headers = parser.getHeaderMap().keySet();
		Class<?> clazz = Class.forName(classForName);
		for (CSVRecord csvRecord : parser.getRecords()) {
			final Object cc = clazz.newInstance();
			for (String header : headers) {
				FieldUtils.writeField(cc, header, csvRecord.get(header), true);

			}
			ret.add((R) cc);
		}

		return ret;
	}
}
