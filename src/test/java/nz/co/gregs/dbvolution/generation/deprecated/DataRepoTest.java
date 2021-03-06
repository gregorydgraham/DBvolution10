/*
 * Copyright 2021 Gregory Graham.
 *
 * Commercial licenses are available, please contact info@gregs.co.nz for details.
 * 
 * This work is licensed under the Creative Commons Attribution-NonCommercial-ShareAlike 4.0 International License. 
 * To view a copy of this license, visit http://creativecommons.org/licenses/by-nc-sa/4.0/ 
 * or send a letter to Creative Commons, PO Box 1866, Mountain View, CA 94042, USA.
 * 
 * You are free to:
 *     Share - copy and redistribute the material in any medium or format
 *     Adapt - remix, transform, and build upon the material
 * 
 *     The licensor cannot revoke these freedoms as long as you follow the license terms.               
 *     Under the following terms:
 *                 
 *         Attribution - 
 *             You must give appropriate credit, provide a link to the license, and indicate if changes were made. 
 *             You may do so in any reasonable manner, but not in any way that suggests the licensor endorses you or your use.
 *         NonCommercial - 
 *             You may not use the material for commercial purposes.
 *         ShareAlike - 
 *             If you remix, transform, or build upon the material, 
 *             you must distribute your contributions under the same license as the original.
 *         No additional restrictions - 
 *             You may not apply legal terms or technological measures that legally restrict others from doing anything the 
 *             license permits.
 * 
 * Check the Creative Commons website for any details, legalese, and updates.
 */
package nz.co.gregs.dbvolution.generation.deprecated;

import java.io.IOException;
import java.sql.SQLException;
import nz.co.gregs.dbvolution.DBRow;
import nz.co.gregs.dbvolution.databases.H2SharedDB;
import nz.co.gregs.dbvolution.generic.AbstractTest;
import org.hamcrest.Matchers;
import static org.hamcrest.Matchers.is;
import org.junit.*;
import static org.junit.Assert.*;

/**
 *
 * @author gregorygraham
 */
public class DataRepoTest extends AbstractTest{
	
	public DataRepoTest(Object testIterationName, Object db) {
		super(testIterationName, db);
	}
	

	@Test
	public void testEasyCompiling() throws SQLException, IOException, Exception {
		if (database instanceof H2SharedDB) {
			// not supported in shared inastances of H2
		} else {
			var generateSchema = DBTableClassGenerator.generateClassesOfViewsAndTables(database, "nz.co.gregs.dbvolution.generation.compiling.dbtableclassgenerator");
			generateSchema.compileWithJavaX();
			// Run separately we get 8 classes
			// but run with other tests we get 50
			assertThat(generateSchema.getRows().size(), Matchers.greaterThanOrEqualTo(8));
		}
	}
	

	@Test
	public void testDataRepoCreation() throws SQLException, IOException, Exception {
		if (database instanceof H2SharedDB) {
			// not supported in shared inastances of H2
		} else {
			var repo = DataRepo.getDataRepoFor(database, "nz.co.gregs.dbvolution.generation.compiling.datarepo");
			// Run separately we get 8 classes
			// but run with other tests we get 50
			assertThat(repo.getRows().size(), Matchers.greaterThanOrEqualTo(8));
			Class<? extends DBRow> rowClass = repo.loadClass("nz.co.gregs.dbvolution.generation.compiling.datarepo.Marque");
			assertThat(rowClass.getCanonicalName(), is("nz.co.gregs.dbvolution.generation.compiling.datarepo.Marque"));
		}
	}
}
