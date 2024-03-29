/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package nz.co.gregs.dbvolution.generation;

import nz.co.gregs.dbvolution.databases.metadata.Options;
import java.io.IOException;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import nz.co.gregs.dbvolution.generic.AbstractTest;
import nz.co.gregs.regexi.Regex;
import org.junit.Test;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.Assert.assertTrue;

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
		if (database.supportsGeometryTypesFullyInSchema()) {
			database.preventDroppingOfTables(false);
			database.dropTableNoExceptions(new Spatialgen());
			database.createTable(new Spatialgen());
			int classesTested = 0;

			Regex regex = Regex
					.startingFromTheBeginning()
					.literal("package nz.co.gregs.dbvolution.generation.tables;\n"
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
							+ "@DBTableName ( value = \"")
					.literalCaseInsensitive("spatialgen") // H2 uppercases table and column names 
					.literal("\" )\n"
							+ "public class Spatialgen extends DBRow { \n"
							+ "\n"
							+ "	public static final Long serialVersionUID = 1L;\n"
							+ "	@DBColumn ( value = \"")
					.literalCaseInsensitive("pk_uid")
					.literal("\" )\n"
							+ "	@DBPrimaryKey\n"
							+ "	@DBAutoIncrement\n"
							+ "	public DBInteger pkUid = new DBInteger();\n"
							+ "	@DBColumn ( value = \"")
					.literalCaseInsensitive("poly")
					.literal("\" )\n"
							+ "	public DBPolygon2D poly = new DBPolygon2D();\n"
							+ "	@DBColumn ( value = \"")
					.literalCaseInsensitive("point")
					.literal("\" )\n"
							+ "	public DBPoint2D point = new DBPoint2D();\n"
							+ "	@DBColumn ( value = \"")
					.literalCaseInsensitive("line")
					.literal("\" )\n"
							+ "	public DBLine2D line = new DBLine2D();\n"
							+ "	@DBColumn ( value = \"")
					.literalCaseInsensitive("mpoint")
					.literal("\" )\n"
							+ "	public DBMultiPoint2D mpoint = new DBMultiPoint2D(); \n"
							+ "\n"
							+ "	public Spatialgen() { \n"
							+ "	} \n"
							+ "\n"
							+ "}")
					.endOfTheString().toRegex();
			ArrayList<Regex> testClasses = new ArrayList<Regex>(1);
			testClasses.add(regex);
			List<String> testClassNames = Arrays.asList(new String[]{"Spatialgen"});
			var generateSchema = DataRepoGenerator.generateClasses(database, "nz.co.gregs.dbvolution.generation", new Options());

			for (DBTableClass dbcl : generateSchema.getTables()) {
				if (testClassNames.contains(dbcl.getClassName())) {
					classesTested++;
					boolean found = false;
					Regex.matchesAll(dbcl.getJavaSource(), testClasses.toArray(new Regex[]{}));
					for (Regex rgx : testClasses) {
						if(rgx.matchesEntireString(dbcl.getJavaSource())){
							found = true;
						}
						System.out.println(dbcl.getJavaSource());
						assertThat(rgx.matches(dbcl.getJavaSource()), is(true));
					}
					assertTrue("Unable to find: \n\"" + dbcl.getJavaSource() + "\"", found);
				}
			}
			assertThat(classesTested, is(1));

			database.preventDroppingOfTables(false);
			database.dropTable(new Spatialgen());
		} else {
			System.out.print("NOT IMPLEMENTED: spatial schema generation for " + database.getLabel() + " has not been implemented");
			System.out.println(" (" + database.getJdbcURL() + ")");
		}
	}
}
