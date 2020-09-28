
package eu.dnetlib.dhp.actionmanager.project;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;

import org.apache.poi.openxml4j.exceptions.InvalidFormatException;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import eu.dnetlib.dhp.actionmanager.project.httpconnector.CollectorServiceException;
import eu.dnetlib.dhp.actionmanager.project.httpconnector.HttpConnector;
import eu.dnetlib.dhp.actionmanager.project.utils.EXCELParser;

public class EXCELParserTest {

	private static Path workingDir;
	private HttpConnector httpConnector = new HttpConnector();
	private static final String URL = "http://cordis.europa.eu/data/reference/cordisref-H2020topics.xlsx";

	@BeforeAll
	public static void beforeAll() throws IOException {
		workingDir = Files.createTempDirectory(CSVParserTest.class.getSimpleName());

	}

	@Test
	public void test1() throws CollectorServiceException, IOException, InvalidFormatException, ClassNotFoundException,
		IllegalAccessException, InstantiationException {

		EXCELParser excelParser = new EXCELParser();

		List<Object> pl = excelParser
			.parse(httpConnector.getInputSourceAsStream(URL), "eu.dnetlib.dhp.actionmanager.project.utils.ExcelTopic");

		System.out.println(pl.size());

//        OPCPackage pkg = OPCPackage.open(httpConnector.getInputSourceAsStream(URL));
//        XSSFWorkbook wb = new XSSFWorkbook(pkg);
//
//        XSSFSheet sheet = wb.getSheet("cordisref-H2020topics");
//
//        DataFormatter dataFormatter = new DataFormatter();
//        Iterator<Row> rowIterator = sheet.rowIterator();
//        List<String> headers = new ArrayList<>();
//        int count = 0;
//        while (rowIterator.hasNext() && count <= 10) {
//            Row row = rowIterator.next();
//
//
//            if(count == 0){
//                // Now let's iterate over the columns of the current row
//                Iterator<Cell> cellIterator = row.cellIterator();
//
//                while(cellIterator.hasNext()){
//                    Cell cell = cellIterator.next();
//                    headers.add(dataFormatter.formatCellValue(cell));
//                }
//            }else{
//                Class<?> clazz = Class.forName("eu.dnetlib.dhp.actionmanager.project.utils.EXCELTopic");
//                final Object cc = clazz.newInstance();
//
//                for(int i =0; i<headers.size(); i++){
//                    Cell cell = row.getCell(i);
//                    FieldUtils.writeField(cc, headers.get(i),dataFormatter.formatCellValue(cell), true);
//
//                }
//
//                System.out.println(new Gson().toJson(cc));
//            }
//
//            count += 1;
//        }
////
////		Iterator<org.apache.poi.ss.usermodel.Sheet> iterator = wb.sheetIterator();
////		System.out.println("Retrieving Sheets using Iterator");
////		while (iterator.hasNext()) {
////			Sheet sheet = iterator.next();
////			System.out.println("=> " + sheet.getSheetName());
////		}
//
//        pkg.close();
	}
}
