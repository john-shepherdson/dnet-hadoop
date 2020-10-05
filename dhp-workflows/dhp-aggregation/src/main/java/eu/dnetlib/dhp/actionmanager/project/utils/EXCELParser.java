
package eu.dnetlib.dhp.actionmanager.project.utils;

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang.reflect.FieldUtils;
import org.apache.poi.openxml4j.exceptions.InvalidFormatException;
import org.apache.poi.openxml4j.opc.OPCPackage;
import org.apache.poi.ss.usermodel.Cell;
import org.apache.poi.ss.usermodel.DataFormatter;
import org.apache.poi.ss.usermodel.Row;
import org.apache.poi.xssf.usermodel.XSSFSheet;
import org.apache.poi.xssf.usermodel.XSSFWorkbook;

/**
 * Reads a generic excel file and maps it into classes that mirror its schema
 */
public class EXCELParser {

	public <R> List<R> parse(InputStream file, String classForName)
		throws ClassNotFoundException, IOException, IllegalAccessException, InstantiationException,
		InvalidFormatException {

		// OPCPackage pkg = OPCPackage.open(httpConnector.getInputSourceAsStream(URL));
		OPCPackage pkg = OPCPackage.open(file);
		XSSFWorkbook wb = new XSSFWorkbook(pkg);

		XSSFSheet sheet = wb.getSheet("cordisref-H2020topics");

		List<R> ret = new ArrayList<>();

		DataFormatter dataFormatter = new DataFormatter();
		Iterator<Row> rowIterator = sheet.rowIterator();
		List<String> headers = new ArrayList<>();
		int count = 0;
		while (rowIterator.hasNext()) {
			Row row = rowIterator.next();

			if (count == 0) {
				Iterator<Cell> cellIterator = row.cellIterator();

				while (cellIterator.hasNext()) {
					Cell cell = cellIterator.next();
					headers.add(dataFormatter.formatCellValue(cell));
				}
			} else {
				Class<?> clazz = Class.forName("eu.dnetlib.dhp.actionmanager.project.utils.EXCELTopic");
				final Object cc = clazz.newInstance();

				for (int i = 0; i < headers.size(); i++) {
					Cell cell = row.getCell(i);
					String value = dataFormatter.formatCellValue(cell);
					FieldUtils.writeField(cc, headers.get(i), dataFormatter.formatCellValue(cell), true);

				}

				EXCELTopic et = (EXCELTopic) cc;
				if (StringUtils.isNotBlank(et.getRcn())) {
					ret.add((R) cc);
				}

			}

			count += 1;
		}

		return ret;
	}

}
