/*
 * Copyright 2023 Gregory Graham.
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
package nz.co.gregs.dbvolution.generic;

import java.sql.SQLException;
import java.util.List;
import nz.co.gregs.dbvolution.DBQuery;
import nz.co.gregs.dbvolution.databases.DBDatabase;
import nz.co.gregs.dbvolution.example.CarCompany;
import nz.co.gregs.dbvolution.example.CompanyLogo;
import nz.co.gregs.dbvolution.example.Marque;
import nz.co.gregs.dbvolution.exceptions.AutoCommitActionDuringTransactionException;
import nz.co.gregs.dbvolution.expressions.ExistsExpression;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import org.junit.Test;

/**
 *
 * @author gregorygraham
 */
public class TempTest extends AbstractTest {

	public TempTest(Object testIterationName, DBDatabase db) throws AutoCommitActionDuringTransactionException, SQLException {
		super(testIterationName, db);

	}

	@Test
	public void testDBExistsOnMultipleTablesUsingDBQueries1() throws SQLException {

		CarCompany carCompany = new CarCompany();
		carCompany.uidCarCompany.permittedValues(3);
		DBQuery existsTables
				= database.getDBQuery()
						.add(carCompany)
						.add(new CompanyLogo());

		Marque marque = new Marque();
		DBQuery outerQuery = database.getDBQuery(marque);

		DBQuery marquesQuery = database.getDBQuery(marque);
		marquesQuery.addCondition(new ExistsExpression(outerQuery, existsTables));

		List<Marque> rowList = marquesQuery.getAllInstancesOf(marque);

		assertThat(rowList.size(), is(0));

		CompanyLogo companyLogo = new CompanyLogo();
		companyLogo.carCompany.setValue(3);
		companyLogo.logoID.setValue(4);
		database.insert(companyLogo);

		rowList = marquesQuery.getAllInstancesOf(marque);

		assertThat(rowList.size(), is(3));

		marquesQuery = database.getDBQuery(marque);
		marquesQuery.addCondition((new ExistsExpression(outerQuery, existsTables)).not());

		rowList = marquesQuery.getAllInstancesOf(marque);

		assertThat(rowList.size(), is(19));
	}

	@Test
	public void testDBExistsOnMultipleTablesUsingDBQueries2() throws SQLException {

		CarCompany carCompany = new CarCompany();
		carCompany.uidCarCompany.permittedValues(3);
		DBQuery existsTables
				= database.getDBQuery()
						.add(carCompany)
						.add(new CompanyLogo());

		Marque marque = new Marque();
		DBQuery outerQuery = database.getDBQuery(marque);

		DBQuery marquesQuery = database.getDBQuery(marque);
		marquesQuery.addCondition(new ExistsExpression(outerQuery, existsTables));

		List<Marque> rowList = marquesQuery.getAllInstancesOf(marque);

		assertThat(rowList.size(), is(0));

		CompanyLogo companyLogo = new CompanyLogo();
		companyLogo.carCompany.setValue(3);
		companyLogo.logoID.setValue(4);
		database.insert(companyLogo);

		rowList = marquesQuery.getAllInstancesOf(marque);

		assertThat(rowList.size(), is(3));

		marquesQuery = database.getDBQuery(marque);
		marquesQuery.addCondition((new ExistsExpression(outerQuery, existsTables)).not());

		rowList = marquesQuery.getAllInstancesOf(marque);

		assertThat(rowList.size(), is(19));
	}

	@Test
	public void testDBExistsOnMultipleTablesUsingDBQueries3() throws SQLException {

		CarCompany carCompany = new CarCompany();
		carCompany.uidCarCompany.permittedValues(3);
		DBQuery existsTables
				= database.getDBQuery()
						.add(carCompany)
						.add(new CompanyLogo());

		Marque marque = new Marque();
		DBQuery outerQuery = database.getDBQuery(marque);

		DBQuery marquesQuery = database.getDBQuery(marque);
		marquesQuery.addCondition(new ExistsExpression(outerQuery, existsTables));

		List<Marque> rowList = marquesQuery.getAllInstancesOf(marque);

		assertThat(rowList.size(), is(0));

		CompanyLogo companyLogo = new CompanyLogo();
		companyLogo.carCompany.setValue(3);
		companyLogo.logoID.setValue(4);
		database.insert(companyLogo);

		rowList = marquesQuery.getAllInstancesOf(marque);

		assertThat(rowList.size(), is(3));

		marquesQuery = database.getDBQuery(marque);
		marquesQuery.addCondition((new ExistsExpression(outerQuery, existsTables)).not());

		rowList = marquesQuery.getAllInstancesOf(marque);

		assertThat(rowList.size(), is(19));
	}

	@Test
	public void testDBExistsOnMultipleTablesUsingDBQueries4() throws SQLException {

		CarCompany carCompany = new CarCompany();
		carCompany.uidCarCompany.permittedValues(3);
		DBQuery existsTables
				= database.getDBQuery()
						.add(carCompany)
						.add(new CompanyLogo());

		Marque marque = new Marque();
		DBQuery outerQuery = database.getDBQuery(marque);

		DBQuery marquesQuery = database.getDBQuery(marque);
		marquesQuery.addCondition(new ExistsExpression(outerQuery, existsTables));

		List<Marque> rowList = marquesQuery.getAllInstancesOf(marque);

		assertThat(rowList.size(), is(0));

		CompanyLogo companyLogo = new CompanyLogo();
		companyLogo.carCompany.setValue(3);
		companyLogo.logoID.setValue(4);
		database.insert(companyLogo);

		rowList = marquesQuery.getAllInstancesOf(marque);

		assertThat(rowList.size(), is(3));

		marquesQuery = database.getDBQuery(marque);
		marquesQuery.addCondition((new ExistsExpression(outerQuery, existsTables)).not());

		rowList = marquesQuery.getAllInstancesOf(marque);

		assertThat(rowList.size(), is(19));
	}
}
