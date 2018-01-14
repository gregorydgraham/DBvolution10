/*
 * Copyright 2015 gregorygraham.
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
package nz.co.gregs.dbvolution.expressions;

import java.io.IOException;
import java.sql.SQLException;
import java.util.List;
import java.util.Set;
import nz.co.gregs.dbvolution.DBQuery;
import nz.co.gregs.dbvolution.DBQueryRow;
import nz.co.gregs.dbvolution.DBRow;
import nz.co.gregs.dbvolution.columns.LargeObjectColumn;
import nz.co.gregs.dbvolution.databases.definitions.DBDefinition;
import nz.co.gregs.dbvolution.datatypes.DBLargeBinary;
import nz.co.gregs.dbvolution.example.CompanyLogo;
import nz.co.gregs.dbvolution.generic.AbstractTest;
import static org.hamcrest.Matchers.*;
import org.junit.Assert;
import org.junit.Test;
import static org.junit.Assert.*;

/**
 *
 * <p style="color: #F90;">Support DBvolution at
 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
 *
 * @author gregorygraham
 */
public class LargeObjectExpressionTest extends AbstractTest {

	public LargeObjectExpressionTest(Object testIterationName, Object db) {
		super(testIterationName, db);
	}

	@Test
	public void testCopy() {
		CompanyLogo companyLogo = new CompanyLogo();
		LargeObjectExpression instance = new LargeObjectExpression(companyLogo.column(companyLogo.imageBytes));
		LargeObjectExpression result = instance.copy();
		final DBDefinition definition = database.getDefinition();
		assertEquals(instance.toSQLString(definition), result.toSQLString(definition));
	}

	@Test
	public void testGetQueryableDatatypeForExpressionValue() {
		CompanyLogo companyLogo = new CompanyLogo();
		LargeObjectColumn instance = companyLogo.column(companyLogo.imageBytes);
		DBLargeBinary expResult = new DBLargeBinary();
		DBLargeBinary result = instance.getQueryableDatatypeForExpressionValue();
		assertEquals(expResult.getClass(), result.getClass());
	}

	@Test
	public void testIsAggregator() {
		LargeObjectExpression instance = new LargeObjectExpression();
		boolean expResult = false;
		boolean result = instance.isAggregator();
		assertEquals(expResult, result);
	}

	@Test
	public void testGetTablesInvolved() {
		CompanyLogo companyLogo = new CompanyLogo();
		LargeObjectExpression instance = new LargeObjectExpression(companyLogo.column(companyLogo.imageBytes));
		Set<DBRow> result = instance.getTablesInvolved();
		DBRow[] resultArray = result.toArray(new DBRow[]{});
		Assert.assertThat(result.size(), is(1));
		Assert.assertThat(resultArray[0].getClass().getSimpleName(), is(companyLogo.getClass().getSimpleName()));
	}

	@Test
	public void testIsNotNull() throws SQLException, IOException {
		CompanyLogo companyLogo = new CompanyLogo();

		DBQuery dbQuery = database.getDBQuery(companyLogo);
		LargeObjectColumn imageBytesColumn = companyLogo.column(companyLogo.imageBytes);
		dbQuery.addCondition(imageBytesColumn.isNotNull());
		List<DBQueryRow> allRows = dbQuery.getAllRows();
		Assert.assertThat(allRows.size(), is(0));

		companyLogo = new CompanyLogo();
		companyLogo.logoID.setValue(1);
		companyLogo.carCompany.setValue(1);//Toyota
		companyLogo.imageFilename.setValue("toyota_logo.jpg");
		companyLogo.imageBytes.setFromFileSystem("toyota_share_logo.jpg");
		database.insert(companyLogo);

		companyLogo = new CompanyLogo();
		companyLogo.logoID.setValue(2);
		companyLogo.carCompany.setValue(2);
		database.insert(companyLogo);

		dbQuery = database.getDBQuery(new CompanyLogo()).setBlankQueryAllowed(true);
		dbQuery.addCondition(imageBytesColumn.isNotNull());
		allRows = dbQuery.getAllRows();

		Assert.assertThat(allRows.size(), is(1));
		Assert.assertThat(allRows.get(0).get(companyLogo).logoID.intValue(), is(1));
	}

	@Test
	public void testIsNull() throws SQLException, IOException {
		CompanyLogo companyLogo = new CompanyLogo();

		DBQuery dbQuery = database.getDBQuery(companyLogo);
		LargeObjectColumn imageBytesColumn = companyLogo.column(companyLogo.imageBytes);
		dbQuery.addCondition(imageBytesColumn.isNotNull());
		List<DBQueryRow> allRows = dbQuery.getAllRows();
		Assert.assertThat(allRows.size(), is(0));

		companyLogo = new CompanyLogo();
		companyLogo.logoID.setValue(1);
		companyLogo.carCompany.setValue(1);//Toyota
		companyLogo.imageFilename.setValue("toyota_logo.jpg");
		companyLogo.imageBytes.setFromFileSystem("toyota_share_logo.jpg");
		database.insert(companyLogo);

		companyLogo = new CompanyLogo();
		companyLogo.logoID.setValue(2);
		companyLogo.carCompany.setValue(2);
		database.insert(companyLogo);

		dbQuery = database.getDBQuery(new CompanyLogo()).setBlankQueryAllowed(true);
		dbQuery.addCondition(imageBytesColumn.isNull());
		allRows = dbQuery.getAllRows();

		Assert.assertThat(allRows.size(), is(1));
		Assert.assertThat(allRows.get(0).get(companyLogo).logoID.intValue(), is(2));
	}

	@Test
	public void testGetIncludesNull() {
		LargeObjectExpression instance = new LargeObjectExpression();
		boolean expResult = false;
		boolean result = instance.getIncludesNull();
		assertEquals(expResult, result);

		instance = new LargeObjectExpression(null);
		expResult = true;
		result = instance.getIncludesNull();
		assertEquals(expResult, result);
	}

	@Test
	public void testIsPurelyFunctional() {
		LargeObjectExpression instance = new LargeObjectExpression();
		boolean result = instance.isPurelyFunctional();
		assertEquals(true, instance.isPurelyFunctional());

		CompanyLogo companyLogo = new CompanyLogo();
		LargeObjectColumn imageBytesColumn = companyLogo.column(companyLogo.imageBytes);
		assertEquals(false, imageBytesColumn.isPurelyFunctional());
	}

}
