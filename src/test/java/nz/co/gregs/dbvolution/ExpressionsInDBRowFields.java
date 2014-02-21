/*
 * Copyright 2014 gregory.graham.
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

import java.util.List;
import nz.co.gregs.dbvolution.annotations.DBColumn;
import nz.co.gregs.dbvolution.datatypes.DBDate;
import nz.co.gregs.dbvolution.datatypes.DBNumber;
import nz.co.gregs.dbvolution.datatypes.DBString;
import nz.co.gregs.dbvolution.example.Marque;
import nz.co.gregs.dbvolution.expressions.DateExpression;
import nz.co.gregs.dbvolution.expressions.NumberExpression;
import nz.co.gregs.dbvolution.expressions.StringExpression;
import nz.co.gregs.dbvolution.generic.AbstractTest;
import static org.hamcrest.Matchers.containsString;
import org.junit.Assert;
import org.junit.Ignore;
import org.junit.Test;

/**
 *
 * @author gregory.graham
 */
public class ExpressionsInDBRowFields extends AbstractTest {

    public ExpressionsInDBRowFields(Object testIterationName, Object db) {
        super(testIterationName, db);
    }

//    @Ignore
    @Test
    public void selectDBRowExpressionWithDBQuery() throws Exception {
        final ExpressionRow exprExample = new ExpressionRow();
        for (String col : exprExample.getColumnNames(database)) {
            System.out.println(col);
        }
        exprExample.name.permittedValuesIgnoreCase("TOYOTA");
        DBQuery query = database.getDBQuery(exprExample);

        final String sqlForQuery = query.getSQLForQuery();
        Assert.assertThat(sqlForQuery, containsString(database.getDefinition().getCurrentDateFunctionName()));

        for (DBQueryRow row : query.getAllRows()) {
            ExpressionRow expressionRow = row.get(exprExample);
            System.out.println(expressionRow.sysDateColumnOnClass.toSQLString(database));
            DBDate currentDate = expressionRow.sysDateColumnOnClass;
            System.out.println("" + currentDate.dateValue());
        }
    }

    @Ignore
    @Test
    public void selectDBRowExpressionWithDBQueryAndExpressionCriteria() throws Exception {
        final ExpressionRow exprExample = new ExpressionRow();
        exprExample.currentUserColumnOnClass.permittedValues("GREGORY");
        for (String col : exprExample.getColumnNames(database)) {
            System.out.println(col);
        }
        exprExample.name.permittedValuesIgnoreCase("TOYOTA");
        DBQuery query = database.getDBQuery(exprExample);

        final String sqlForQuery = query.getSQLForQuery();
        Assert.assertThat(sqlForQuery, containsString(database.getDefinition().getCurrentDateFunctionName()));

        for (DBQueryRow row : query.getAllRows()) {
            ExpressionRow expressionRow = row.get(new ExpressionRow());
            System.out.println(expressionRow.sysDateColumnOnClass.toSQLString(database));
            DBDate currentDate = expressionRow.sysDateColumnOnClass;
            System.out.println("" + currentDate.dateValue());
        }
    }

    @Ignore
    @Test
    public void selectDBRowExpressionWithDBTable() throws Exception {
        final ExpressionRow exprExample = new ExpressionRow();
        exprExample.name.permittedValuesIgnoreCase("TOYOTA");
        DBTable<ExpressionRow> table = database.getDBTable(exprExample);

        final String sqlForQuery = table.getSQLForQuery();
        System.out.println(sqlForQuery);
        Assert.assertThat(sqlForQuery, containsString(database.getDefinition().getCurrentDateFunctionName()));
        final List<ExpressionRow> rowsByExample = table.getAllRows();
        database.print(rowsByExample);

        for (ExpressionRow expressionRow : rowsByExample) {
            System.out.println(expressionRow.sysDateColumnOnClass.toSQLString(database));
            System.out.println("" + expressionRow.sysDateColumnOnClass.dateValue());
        }
    }

    public static class ExpressionRow extends Marque {

        public static final long serialVersionUID = 1L;
        @DBColumn
        DBDate sysDateColumnOnClass = new DBDate(DateExpression.currentDate());

        @DBColumn
        DBString currentUserColumnOnClass = new DBString(StringExpression.currentUser());

        @DBColumn
        DBNumber numberColumnOnClass = new DBNumber(NumberExpression.value(5).times(3));

    }

}
