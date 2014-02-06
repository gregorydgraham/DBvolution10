/*
 * Copyright 2013 gregorygraham.
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

import nz.co.gregs.dbvolution.DBQuery;
import nz.co.gregs.dbvolution.DBRow;
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

        String expectedResult =
                "SELECT __296612642.UIDCOMPANY DB_1165478727, \n"
                + "__296612642.FKSTATISTIC2 DB_779023597, \n"
                + "__77293264.UIDSTATISTIC DB_715138364, \n"
                + "__77293264.STAT2ID DB1041270709\n"
                + " FROM Company AS __296612642 \n"
                + " INNER JOIN Statistic AS __77293264 ON( __296612642.FKSTATISTIC2 = __77293264.UIDSTATISTIC )\n"
                + " WHERE 1=1 \n"
                + " AND (__296612642.UIDCOMPANY = 234)\n"
                + ";";

        System.out.println(expectedResult);
        System.out.println(generateSQLString);
        assertThat(dbQuery.isUseANSISyntax(), is(true));
        assertThat(testableSQLWithoutColumnAliases(expectedResult),
                is(testableSQLWithoutColumnAliases(generateSQLString)));
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

        String expectedResult =
                "select "
                + "__296612642.uidcompany, "
                + "__296612642.fkstatistic2, "
                + "__77293264.uidstatistic, "
                + "__77293264.stat2id "
                + "from company, statistic "
                + "where 1=1 "
                + "and (__296612642.uidcompany = 234) "
                + "and (__296612642.fkstatistic2 = __77293264.uidstatistic)"
                + " ;";

        System.out.println(expectedResult);
        System.out.println(generateSQLString);
        assertThat(testableSQLWithoutColumnAliases(expectedResult),
                is(testableSQLWithoutColumnAliases(generateSQLString)));
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

        String expectedResult =
                " SELECT __1641109531.UIDCOMPANY DB_1808204696, \n"
                + "__1641109531.FKSTATISTIC2 DB36610818, \n"
                + "__77293264.UIDSTATISTIC DB_715138364, \n"
                + "__77293264.STAT2ID DB1041270709\n"
                + " FROM  Company AS __1641109531 \n"
                + " INNER JOIN Statistic AS __77293264  ON( __1641109531.FKSTATISTIC2 = __77293264.STAT2ID )\n"
                + " WHERE  1=1 \n"
                + " AND (__1641109531.UIDCOMPANY = 234)\n"
                + ";";

        System.out.println(expectedResult);
        System.out.println(generateSQLString);
        assertThat(dbQuery.isUseANSISyntax(), is(true));
        assertThat(testableSQLWithoutColumnAliases(expectedResult), is(testableSQLWithoutColumnAliases(generateSQLString)));
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

        String expectedResult =
                "select"
                + " __1641109531.uidcompany, "
                + "__1641109531.fkstatistic2, "
                + "__77293264.uidstatistic, "
                + "__77293264.stat2id "
                + "from company, statistic "
                + "where 1=1 "
                + "and (__1641109531.uidcompany = 234) "
                + "and (__1641109531.fkstatistic2 = __77293264.stat2id) "
                + ";";

        System.out.println(expectedResult);
        System.out.println(generateSQLString);
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
