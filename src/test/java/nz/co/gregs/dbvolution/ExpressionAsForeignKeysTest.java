/*
 * Copyright 2014 gregorygraham.
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

import nz.co.gregs.dbvolution.example.CarCompany;
import nz.co.gregs.dbvolution.example.Marque;
import nz.co.gregs.dbvolution.generic.AbstractTest;
import org.hamcrest.Matchers;
import static org.hamcrest.Matchers.*;
import org.junit.Assert;
import org.junit.Test;

/**
 *
 * @author gregorygraham
 */
public class ExpressionAsForeignKeysTest extends AbstractTest {

	public ExpressionAsForeignKeysTest(Object testIterationName, Object db) {
		super(testIterationName, db);
	}

	@Test
	public void testSimpleExpressionsAsForeignKeysDoesntThrowCartesianJoinException() {

		DBQuery dbQuery = database.getDBQuery();
		CarCompany carCompany = new CarCompany();
		carCompany.name.permittedValues("TOYOTA");
		carCompany.ignoreAllForeignKeys();
		final Marque marque = new Marque();
		marque.ignoreAllForeignKeys();
		dbQuery.add(marque);
		dbQuery.add(carCompany);
		dbQuery.addCondition(marque.column(marque.carCompany).is(carCompany.column(carCompany.uidCarCompany)));
        final String generateSQLString = dbQuery.getSQLForQuery();

		String expectedResult1 = "select __1997432637.numeric_code, __1997432637.uid_marque, __1997432637.isusedfortafros, __1997432637.fk_toystatusclass, __1997432637.intindallocallowed, __1997432637.upd_count, __1997432637.auto_created, __1997432637.name, __1997432637.pricingcodeprefix, __1997432637.reservationsalwd, __1997432637.creation_date, __1997432637.enabled, __1997432637.fk_carcompany, __78874071.name, __78874071.uid_carcompany from marque as __1997432637 inner join car_company as __78874071 on( ((__78874071.name = 'toyota')) and (__1997432637.fk_carcompany = __78874071.uid_carcompany) ) ;";
		String expectedResult2 = "select __1997432637.numeric_code, __1997432637.uid_marque, __1997432637.isusedfortafros, __1997432637.fk_toystatusclass, __1997432637.intindallocallowed, __1997432637.upd_count, __1997432637.auto_created, __1997432637.name, __1997432637.pricingcodeprefix, __1997432637.reservationsalwd, __1997432637.creation_date, __1997432637.enabled, __1997432637.fk_carcompany, __78874071.name, __78874071.uid_carcompany from marque as __1997432637 inner join car_company as __78874071 on( __1997432637.fk_carcompany = __78874071.uid_carcompany ) where 1=1 and (__78874071.name = 'toyota') ;";
		String expectedResult3 = "select __78874071.name, __78874071.uid_carcompany, __1997432637.numeric_code, __1997432637.uid_marque, __1997432637.isusedfortafros, __1997432637.fk_toystatusclass, __1997432637.intindallocallowed, __1997432637.upd_count, __1997432637.auto_created, __1997432637.name, __1997432637.pricingcodeprefix, __1997432637.reservationsalwd, __1997432637.creation_date, __1997432637.enabled, __1997432637.fk_carcompany from car_company as __78874071 inner join marque as __1997432637 on( __1997432637.fk_carcompany = __78874071.uid_carcompany ) where 1=1 and (__78874071.name = 'toyota') ;";
		String expectedResult4 = "select oo78874071.name, oo78874071.uid_carcompany, oo1997432637.numeric_code, oo1997432637.uid_marque, oo1997432637.isusedfortafros, oo1997432637.fk_toystatusclass, oo1997432637.intindallocallowed, oo1997432637.upd_count, oo1997432637.auto_created, oo1997432637.name, oo1997432637.pricingcodeprefix, oo1997432637.reservationsalwd, oo1997432637.creation_date, oo1997432637.enabled, oo1997432637.fk_carcompany from car_company as oo78874071 inner join marque as oo1997432637 on( oo1997432637.fk_carcompany = oo78874071.uid_carcompany ) where 1=1 and (oo78874071.name = 'toyota') ;";

		System.out.println(expectedResult1);
		System.out.println(generateSQLString);
		Assert.assertThat(
				testableSQLWithoutColumnAliases(generateSQLString),
				anyOf(
						is(testableSQLWithoutColumnAliases(expectedResult1)),
						is(testableSQLWithoutColumnAliases(expectedResult2)),
						is(testableSQLWithoutColumnAliases(expectedResult3)),
						is(testableSQLWithoutColumnAliases(expectedResult4))
				));
	}

}
