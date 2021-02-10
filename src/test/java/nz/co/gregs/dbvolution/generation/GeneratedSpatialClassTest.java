/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package nz.co.gregs.dbvolution.generation;

import java.io.IOException;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import nz.co.gregs.dbvolution.databases.MySQLDB;
import nz.co.gregs.dbvolution.databases.PostgresDB;
import nz.co.gregs.dbvolution.generic.AbstractTest;
import static org.hamcrest.MatcherAssert.assertThat;
import org.junit.Assert;
import org.junit.Test;
import static org.hamcrest.Matchers.is;

/**
 *
 * @author gregorygraham
 */
public class GeneratedSpatialClassTest extends AbstractTest {

	public GeneratedSpatialClassTest(Object testIterationName, Object db) {
		super(testIterationName, db);
	}

	@Test
	public void testGetSchema() throws SQLException, IOException {
		if ((database instanceof MySQLDB) || (database instanceof PostgresDB)) {
			database.preventDroppingOfTables(false);
			database.dropTableNoExceptions(new Spatialgen());
			database.createTable(new Spatialgen());
			int classesTested = 0;

			List<String> testClassNames = Arrays.asList(new String[]{"Spatialgen"});
			List<String> testClasses = new ArrayList<String>();
			testClasses.add(
					"package nz.co.gregs.dbvolution.generation.tables;\n"
					+ "\n"
					+ "import java.lang.Long;\n"
					+ "import nz.co.gregs.dbvolution.DBRow;\n"
					+ "import nz.co.gregs.dbvolution.annotations.DBAutoIncrement;\n"
					+ "import nz.co.gregs.dbvolution.annotations.DBColumn;\n"
					+ "import nz.co.gregs.dbvolution.annotations.DBPrimaryKey;\n"
					+ "import nz.co.gregs.dbvolution.annotations.DBTableName;\n"
					+ "import nz.co.gregs.dbvolution.datatypes.DBInteger;\n"
					+ "import nz.co.gregs.dbvolution.datatypes.spatial2D.DBLine2D;\n"
					+ "import nz.co.gregs.dbvolution.datatypes.spatial2D.DBMultiPoint2D;\n"
					+ "import nz.co.gregs.dbvolution.datatypes.spatial2D.DBPoint2D;\n"
					+ "import nz.co.gregs.dbvolution.datatypes.spatial2D.DBPolygon2D;\n"
					+ "\n"
					+ "@DBTableName ( value = \"spatialgen\" )\n"
					+ "public class Spatialgen extends DBRow { \n"
					+ "\n"
					+ "	public static final Long serialVersionUID = 1L;\n"
					+ "	@DBColumn ( value = \"pk_uid\" )\n"
					+ "	@DBPrimaryKey\n"
					+ "	@DBAutoIncrement\n"
					+ "	public DBInteger pkUid = new DBInteger();\n"
					+ "	@DBColumn ( value = \"poly\" )\n"
					+ "	public DBPolygon2D poly = new DBPolygon2D();\n"
					+ "	@DBColumn ( value = \"point\" )\n"
					+ "	public DBPoint2D point = new DBPoint2D();\n"
					+ "	@DBColumn ( value = \"line\" )\n"
					+ "	public DBLine2D line = new DBLine2D();\n"
					+ "	@DBColumn ( value = \"mpoint\" )\n"
					+ "	public DBMultiPoint2D mpoint = new DBMultiPoint2D(); \n"
					+ "\n"
					+ "	public Spatialgen() { \n"
					+ "	} \n"
					+ "\n"
					+ "}");
			var generateSchema = DataRepoGenerator.generateClasses(database, "nz.co.gregs.dbvolution.generation", new Options());

			for (DBTableClass dbcl : generateSchema.getTables()) {
				if (testClassNames.contains(dbcl.getClassName())) {
					classesTested++;
					boolean found = false;
					for (String str : testClasses) {
						System.out.println("" + dbcl.getJavaSource());
//						final String testcaseLowercase = str.toLowerCase().replaceAll("[ \n\r\t]+", " ");
//						final String sourceLowercase = dbcl.getJavaSource().toLowerCase().replaceAll("[ \n\r\t]+", " ");
//						assertThat(sourceLowercase, is(testcaseLowercase));
						assertThat(dbcl.getJavaSource(), is(str));
						if (dbcl.getJavaSource().equals(str)) {
							found = true;
						}
					}
					Assert.assertTrue("Unable to find: \n\"" + dbcl.getJavaSource() + "\"", found);
				}
			}
			assertThat(classesTested, is(1));

			database.preventDroppingOfTables(false);
			database.dropTable(new Spatialgen());
		}
	}
}
