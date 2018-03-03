/*
 * Copyright 2013 Gregory Graham.
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

import static org.hamcrest.Matchers.*;
import static org.junit.Assert.*;

import java.sql.SQLException;
import nz.co.gregs.dbvolution.annotations.DBColumn;
import nz.co.gregs.dbvolution.annotations.DBForeignKey;
import nz.co.gregs.dbvolution.annotations.DBPrimaryKey;
import nz.co.gregs.dbvolution.annotations.DBTableName;
import nz.co.gregs.dbvolution.datatypes.DBInteger;
import nz.co.gregs.dbvolution.generic.AbstractTest;

import org.junit.Test;

/**
 *
 */
public class JoinTest extends AbstractTest {

	public JoinTest(Object testIterationName, Object db) {
		super(testIterationName, db);
	}

	@Test
	public void testQueryGenerationUsingANSIGivenFkToPk() throws SQLException {
		DBQuery dbQuery = database.getDBQuery();
		CompanyWithFkToPk companyExample = new CompanyWithFkToPk();
		companyExample.uid.permittedValues(234L);
		dbQuery.add(companyExample);
		dbQuery.add(new Statistic());
		dbQuery.setUseANSISyntax(true);
		final String generateSQLString = dbQuery.getSQLForQuery().replaceAll(" +", " ");

		String expectedResult1
				= "select __296612642.uidcompany, __296612642.fkstatistic2, __77293264.uidstatistic, __77293264.stat2id from company as __296612642 inner join statistic as __77293264 on( ((__296612642.uidcompany = 234)) and (__296612642.fkstatistic2 = __77293264.uidstatistic) ) ;";
		String expectedResult2
				= "select __296612642.uidcompany, __296612642.fkstatistic2, __77293264.uidstatistic, __77293264.stat2id from company as __296612642 inner join statistic as __77293264 on( __296612642.fkstatistic2 = __77293264.uidstatistic ) where (1=1) and (__296612642.uidcompany = 234) ;";
		String expectedResult3
				= "select oo296612642.uidcompany, oo296612642.fkstatistic2, oo77293264.uidstatistic, oo77293264.stat2id from company as oo296612642 inner join statistic as oo77293264 on( oo296612642.fkstatistic2 = oo77293264.uidstatistic ) where (1=1) and (oo296612642.uidcompany = 234) ;";
		assertThat(dbQuery.isUseANSISyntax(), is(true));
		assertThat(testableSQLWithoutColumnAliases(generateSQLString),
				anyOf(
						is(testableSQLWithoutColumnAliases(expectedResult1)),
						is(testableSQLWithoutColumnAliases(expectedResult2)),
						is(testableSQLWithoutColumnAliases(expectedResult3))
				)
		);
	}

	@Test
	public void testQueryGenerationUsingNonANSIGivenFkToPk() throws SQLException {
		DBQuery dbQuery = database.getDBQuery();
		CompanyWithFkToPk companyExample = new CompanyWithFkToPk();
		companyExample.uid.permittedValues(234L);
		dbQuery.add(companyExample);
		dbQuery.add(new Statistic());
		dbQuery.setUseANSISyntax(false);
		final String generateSQLString = dbQuery.getSQLForQuery().replaceAll(" +", " ");

		String expectedResult
				= "select "
				+ "__296612642.uidcompany, "
				+ "__296612642.fkstatistic2, "
				+ "__77293264.uidstatistic, "
				+ "__77293264.stat2id "
				+ "from company, statistic "
				+ "where (1=1) "
				+ "and (__296612642.uidcompany = 234) "
				+ "and (__296612642.fkstatistic2 = __77293264.uidstatistic)"
				+ " ;";

		assertThat(testableSQLWithoutColumnAliases(generateSQLString),
				is(testableSQLWithoutColumnAliases(expectedResult)));
	}

	@Test
	public void testQueryGenerationUsingANSIGivenFkToNonPk() throws SQLException {
		DBQuery dbQuery = database.getDBQuery();
		CompanyWithFkToNonPk companyExample = new CompanyWithFkToNonPk();
		companyExample.uid.permittedValues(234L);
		dbQuery.add(companyExample);
		dbQuery.add(new Statistic());
		dbQuery.setUseANSISyntax(true);
		final String generateSQLString = dbQuery.getSQLForQuery().replaceAll(" +", " ");

		String expectedResult3
				= "select __1641109531.uidcompany, __1641109531.fkstatistic2, __77293264.uidstatistic, __77293264.stat2id from company as __1641109531 inner join statistic as __77293264 on( __1641109531.fkstatistic2 = __77293264.stat2id ) where (1=1) and (__1641109531.uidcompany = 234) ;";
		assertThat(dbQuery.isUseANSISyntax(), is(true));
		assertThat(testableSQLWithoutColumnAliases(generateSQLString),
				is(testableSQLWithoutColumnAliases(expectedResult3))
		);
	}

	@Test
	public void testQueryGenerationUsingNonANSIGivenFkToNonPk() throws SQLException {
		DBQuery dbQuery = database.getDBQuery();
		CompanyWithFkToNonPk companyExample = new CompanyWithFkToNonPk();
		companyExample.uid.permittedValues(234L);
		dbQuery.add(companyExample);
		dbQuery.add(new Statistic());
		dbQuery.setUseANSISyntax(false);
		final String generateSQLString = dbQuery.getSQLForQuery().replaceAll(" +", " ");

		String expectedResult
				= "select"
				+ " __1641109531.uidcompany, "
				+ "__1641109531.fkstatistic2, "
				+ "__77293264.uidstatistic, "
				+ "__77293264.stat2id "
				+ "from company, statistic "
				+ "where (1=1) "
				+ "and (__1641109531.uidcompany = 234) "
				+ "and (__1641109531.fkstatistic2 = __77293264.stat2id) "
				+ ";";

		assertThat(testableSQLWithoutColumnAliases(expectedResult),
				is(testableSQLWithoutColumnAliases(generateSQLString)));
	}

	@DBTableName("Company")
	public static class CompanyWithFkToPk extends DBRow {

		private static final long serialVersionUID = 1L;
		@DBColumn("uidCompany")
		@DBPrimaryKey
		public DBInteger uid = new DBInteger();
		@DBForeignKey(value = Statistic.class)
		@DBColumn("fkStatistic2")
		public DBInteger carCompany = new DBInteger();
	}

	@DBTableName("Company")
	public static class CompanyWithFkToNonPk extends DBRow {

		private static final long serialVersionUID = 1L;
		@DBColumn("uidCompany")
		@DBPrimaryKey
		public DBInteger uid = new DBInteger();
		@DBForeignKey(value = Statistic.class, column = "stat2Id")
		@DBColumn("fkStatistic2")
		public DBInteger carCompany = new DBInteger();
	}

	public static class Statistic extends DBRow {

		private static final long serialVersionUID = 1L;
		@DBColumn("uidStatistic")
		@DBPrimaryKey
		public DBInteger uid = new DBInteger();
		@DBColumn("stat2Id")
		public DBInteger stat2Id = new DBInteger();
	}
}
