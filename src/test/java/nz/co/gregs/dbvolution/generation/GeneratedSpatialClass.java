/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package nz.co.gregs.dbvolution.generation;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import nz.co.gregs.dbvolution.databases.MySQLDB;
import nz.co.gregs.dbvolution.databases.PostgresDB;
import nz.co.gregs.dbvolution.generic.AbstractTest;
import org.junit.Assert;
import org.junit.Test;
import static org.hamcrest.Matchers.is;

/**
 *
 * <p style="color: #F90;">Support DBvolution at
 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
 *
 * @author gregorygraham
 */
public class GeneratedSpatialClass extends AbstractTest {

	public GeneratedSpatialClass(Object testIterationName, Object db) {
		super(testIterationName, db);
	}

	@Test
	public void testGetSchema() throws SQLException {
		if ((database instanceof MySQLDB) || (database instanceof PostgresDB)) {
			database.preventDroppingOfTables(false);
			database.dropTableNoExceptions(new Spatialgen());
			database.createTable(new Spatialgen());
			int classesTested = 0;
			List<DBTableClass> generateSchema;
			List<String> testClassNames = Arrays.asList(new String[]{"Spatialgen"});
			List<String> testClasses = new ArrayList<String>();
			testClasses.add("package nz.co.gregs.dbvolution.generation;\n"
					+ "\n"
					+ "import nz.co.gregs.dbvolution.*;\n"
					+ "import nz.co.gregs.dbvolution.datatypes.*;\n"
					+ "import nz.co.gregs.dbvolution.datatypes.spatial2D.*;\n"
					+ "import nz.co.gregs.dbvolution.annotations.*;\n"
					+ "\n"
					+ "@DBTableName(\"spatialgen\") \n"
					+ "public class Spatialgen extends DBRow {\n"
					+ "\n"
					+ "    public static final long serialVersionUID = 1L;\n"
					+ "\n"
					+ "    @DBColumn(\"pk_uid\")\n"
					+ "    @DBPrimaryKey\n"
					+ "    @DBAutoIncrement\n"
					+ "    public DBInteger pkUid = new DBInteger();\n"
					+ "\n"
					+ "    @DBColumn(\"poly\")\n"
					+ "    public DBPolygon2D poly = new DBPolygon2D();\n"
					+ "\n"
					+ "    @DBColumn(\"point\")\n"
					+ "    public DBPoint2D point = new DBPoint2D();\n"
					+ "\n"
					+ "    @DBColumn(\"line\")\n"
					+ "    public DBLine2D line = new DBLine2D();\n"
					+ "\n"
					+ "    @DBColumn(\"mpoint\")\n"
					+ "    public DBMultiPoint2D mpoint = new DBMultiPoint2D();\n"
					+ "\n"
					+ "}\n"
					+ "");
			generateSchema = DBTableClassGenerator.generateClassesOfTables(database, "nz.co.gregs.dbvolution.generation", new DBTableClassGenerator.Options());
			for (DBTableClass dbcl : generateSchema) {
				if (testClassNames.contains(dbcl.getClassName())) {
					classesTested++;
					boolean found = false;
					for (String str : testClasses) {
						final String testcaseLowercase = str.toLowerCase().replaceAll("[ \n\r\t]+", " ");
						final String sourceLowercase = dbcl.getJavaSource().toLowerCase().replaceAll("[ \n\r\t]+", " ");
						Assert.assertThat(sourceLowercase, is(testcaseLowercase));
						if (testcaseLowercase.equals(sourceLowercase)) {
							found = true;
						}
					}
					Assert.assertTrue("Unable to find: \n\"" + dbcl.getJavaSource() + "\"", found);
				}
			}
			Assert.assertThat(classesTested, is(1));

			database.preventDroppingOfTables(false);
			database.dropTable(new Spatialgen());
		}
	}
}
