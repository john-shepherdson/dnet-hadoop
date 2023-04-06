
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

import eu.dnetlib.dhp.actionmanager.project.utils.model.EXCELTopic;

/**
 * Reads a generic excel file and maps it into classes that mirror its schema
 */
@Deprecated
public class EXCELParser {

	public <R> List<R> parse(InputStream file, String classForName, String sheetName)
		throws ClassNotFoundException, IOException, IllegalAccessException, InstantiationException,
		InvalidFormatException {

		try (OPCPackage pkg = OPCPackage.open(file); XSSFWorkbook wb = new XSSFWorkbook(pkg)) {

			XSSFSheet sheet = wb.getSheet(sheetName);

			if (sheet == null) {
				throw new IllegalArgumentException("Sheet name " + sheetName + " not present in current file");
			}

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
					Class<?> clazz = Class.forName(classForName);
					final Object cc = clazz.newInstance();

					for (int i = 0; i < headers.size(); i++) {
						Cell cell = row.getCell(i);
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

}
