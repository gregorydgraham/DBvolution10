/*
 * Copyright 2014 greg.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package nz.co.gregs.dbvolution;

import java.sql.SQLException;
import java.util.List;
import nz.co.gregs.dbvolution.example.CarCompany;
import nz.co.gregs.dbvolution.example.CompanyLogo;
import nz.co.gregs.dbvolution.example.LinkCarCompanyAndLogoWithPreviousLink;
import nz.co.gregs.dbvolution.example.Marque;
import nz.co.gregs.dbvolution.generic.AbstractTest;
import org.junit.Assert;
import org.junit.Test;
import static org.hamcrest.Matchers.*;

/**
 *
 * <p style="color: #F90;">Support DBvolution at
 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
 *
 * @author greg
 */
public class MatchAnyTests extends AbstractTest {

	public MatchAnyTests(Object testIterationName, Object db) {
		super(testIterationName, db);
	}

	@Test
	public void testSimpleTableQuery() throws SQLException {
		Marque marq = new Marque();
		marq.name.permittedValuesIgnoreCase("toyota");
		marq.uidMarque.permittedValues(2);
		DBTable<Marque> dbTable = database.getDBTable(marq);
		List<Marque> marquesFound = dbTable.getAllRows();
		Assert.assertThat(marquesFound.size(), is(0));

		dbTable.setToMatchAnyCondition();
		marquesFound = dbTable.getRowsByExample(marq);
		Assert.assertThat(marquesFound.size(), is(2));
	}

	@Test
	public void testSimpleQuery() throws SQLException {
		Marque marq = new Marque();
		marq.name.permittedValuesIgnoreCase("toyota");
		marq.uidMarque.permittedValues(2);
		DBQuery dbQuery = database.getDBQuery(marq);
		List<Marque> marquesFound = dbQuery.getAllInstancesOf(marq);
		Assert.assertThat(marquesFound.size(), is(0));

		dbQuery.setToMatchAnyCondition();
		marquesFound = dbQuery.getAllInstancesOf(marq);
		Assert.assertThat(marquesFound.size(), is(2));
	}

	@Test
	public void testSimpleQueryJoin() throws SQLException {
		Marque marq = new Marque();
		CarCompany carCo = new CarCompany();
		marq.name.permittedValuesIgnoreCase("toyota");
		marq.uidMarque.permittedValues(2);
		DBQuery dbQuery = database.getDBQuery(marq, carCo);
		List<Marque> marquesFound = dbQuery.getAllInstancesOf(marq);
		Assert.assertThat(marquesFound.size(), is(0));

		dbQuery.setToMatchAnyCondition();
		marquesFound = dbQuery.getAllInstancesOf(marq);
		Assert.assertThat(marquesFound.size(), is(2));
	}

	@Test
	public void testJoinOfAnyRelationshipWorksForSingleRelationship() throws SQLException {
		Marque marq = new Marque();
		CarCompany carCo = new CarCompany();
		marq.name.permittedValuesIgnoreCase("toyota");
		marq.uidMarque.permittedValues(2);
		DBQuery dbQuery = database.getDBQuery(marq, carCo);
		List<Marque> marquesFound = dbQuery.getAllInstancesOf(marq);
		Assert.assertThat(marquesFound.size(), is(0));

		dbQuery.setToMatchAnyRelationship();
		marquesFound = dbQuery.getAllInstancesOf(marq);
		Assert.assertThat(marquesFound.size(), is(0));
	}

	@Test
	public void testJoinOfAnyRelationshipWorksFor2Relationships() throws SQLException {
		LinkCarCompanyAndLogoWithPreviousLink link = new LinkCarCompanyAndLogoWithPreviousLink();
		CompanyLogo logo = new CompanyLogo();
		DBQuery dbQuery = database.getDBQuery(link, logo);

		dbQuery.setToMatchAnyRelationship();
		Assert.assertThat(
				testableSQL(dbQuery.getSQLForQuery()),
				containsString(testableSQL("ON( __2076078474.FK_COMPANY_LOGO = _1159239592.LOGO_ID OR __2076078474.FK_PREV_COMPANY_LOGO = _1159239592.LOGO_ID )"))
		);
	}

}
