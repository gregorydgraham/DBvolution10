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

import java.lang.reflect.Field;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import nz.co.gregs.dbvolution.columns.ColumnProvider;
import nz.co.gregs.dbvolution.datatypes.QueryableDatatype;
import nz.co.gregs.dbvolution.expressions.DBExpression;
import nz.co.gregs.dbvolution.query.RowDefinition;

/**
 *
 * @author gregory.graham
 */
public class DBReport extends RowDefinition{
    private static final long serialVersionUID = 1L;

    protected ColumnProvider[] sortColumns = new ColumnProvider[]{};

    public DBReport() {
        super();
    }

    /**
     * Gets all the report rows of the supplied DBReport using only conditions
     * supplied within the supplied DBReport.
     *
     * <p>
     * Use this method to retrieve all rows when the criteria have been supplied
     * as part of the DBReport subclass.
     *
     * <p>
     * If you require extra criteria to be add to the DBReport, limiting the
     * results to a subset, use the
     * {@link DBReport#getRows(nz.co.gregs.dbvolution.DBDatabase, nz.co.gregs.dbvolution.DBReport, nz.co.gregs.dbvolution.DBRow...) getRows method}.
     *
     * @param <A>
     * @param database
     * @param exampleReport
     * @return a list of DBReport instances representing the results of the
     * report query.
     * @throws SQLException
     */
    public static <A extends DBReport> List<A> getAllRows(DBDatabase database, A exampleReport) throws SQLException {
        DBQuery query = setUpQuery(database, exampleReport, new DBRow[]{});
        List<A> reportRows = new ArrayList<A>();
        query.setBlankQueryAllowed(true);
        List<DBQueryRow> allRows = query.getAllRows();
        for (DBQueryRow row : allRows) {
            reportRows.add(DBReport.getReportInstance(exampleReport, row));
        }
        return reportRows;
    }

    /**
     * Gets all the report rows of the supplied DBReport using the supplied
     * example rows.
     *
     * All supplied rows should be from a DBRow subclass that is included in the
     * report.
     *
     * @param <A>
     * @param database
     * @param exampleReport
     * @param rows
     * @return a list of DBReport instances representing the results of the
     * report query.
     * @throws SQLException
     */
    public static <A extends DBReport> List<A> getRows(DBDatabase database, A exampleReport, DBRow... rows) throws SQLException {
        DBQuery query = setUpQuery(database, exampleReport, rows);
        List<A> reportRows = new ArrayList<A>();
        List<DBQueryRow> allRows = query.getAllRows();
        for (DBQueryRow row : allRows) {
            reportRows.add(DBReport.getReportInstance(exampleReport, row));
        }
        return reportRows;
    }

    public static <A extends DBReport> String getSQLForQuery(DBDatabase database, A exampleReport, DBRow... rows) throws SQLException {
        DBQuery query = setUpQuery(database, exampleReport, rows);
        return query.getSQLForQuery();
    }

    public static <A extends DBReport> String getSQLForCount(DBDatabase database, A exampleReport, DBRow... rows) throws SQLException {
        DBQuery query = setUpQuery(database, exampleReport, rows);
        return query.getSQLForCount();
    }

    @Override
    public String toString() {
        StringBuilder string = new StringBuilder();
        Field[] fields = this.getClass().getDeclaredFields();

        String separator = "";

        for (Field field : fields) {
            try {
                final Object get = field.get(this);
                if (QueryableDatatype.class.isAssignableFrom(get.getClass())) {
                    string.append(separator);
                    string.append(" ");
                    string.append(field.getName());
                    string.append(":");
                    string.append(get.toString());
                    separator = ",";
                }
            } catch (IllegalArgumentException ex) {
                throw new RuntimeException(ex);
            } catch (IllegalAccessException ex) {
                throw new RuntimeException(ex);
            }
        }
        return string.toString();
    }

    public void setSortOrder(QueryableDatatype... columns) {
        List<ColumnProvider> columnProviders = new ArrayList<ColumnProvider>();
        for (QueryableDatatype qdt : columns) {
                columnProviders.add(this.column(qdt));
        }
        sortColumns = columnProviders.toArray(new ColumnProvider[]{});
    }

    private static <A extends DBReport> DBQuery setUpQuery(DBDatabase database, A exampleReport, DBRow[] rows) {
        DBQuery query = database.getDBQuery();
        addTablesAndExpressions(query, exampleReport);
        query.addExtraExamples(rows);
        query.setSortOrder(exampleReport.sortColumns);
        return query;
    }

    private static <A extends DBReport> void addTablesAndExpressions(DBQuery query, A exampleReport) {
        Field[] fields = exampleReport.getClass().getFields();
        if (fields.length == 0) {
            throw new UnableToAccessDBReportFieldException(exampleReport, null, null);
        }
        for (Field field : fields) {
            final Object value;
            try {
                value = field.get(exampleReport);
                if (value != null && DBRow.class.isAssignableFrom(value.getClass())) {
                    if (value instanceof DBRow) {
                        final DBRow dbRow = (DBRow) value;
                        dbRow.removeAllFieldsFromResults();
                        query.add(dbRow);
                    }
                } else if (value != null && QueryableDatatype.class.isAssignableFrom(value.getClass())) {
                    if ((value instanceof QueryableDatatype) && ((QueryableDatatype) value).hasColumnExpression()) {
                        final DBExpression columnExpression = ((QueryableDatatype) value).getColumnExpression();
                        query.addExpressionColumn(value, columnExpression);
                        if (!columnExpression.isAggregator()) {
                            query.addGroupByColumn(value, columnExpression);
                        }
                    }
                }
            } catch (IllegalArgumentException ex) {
                throw new UnableToAccessDBReportFieldException(exampleReport, field, ex);
            } catch (IllegalAccessException ex) {
                throw new UnableToAccessDBReportFieldException(exampleReport, field, ex);
            }
        }
    }

    @SuppressWarnings("unchecked")
    private static <A extends DBReport> A getReportInstance(A exampleReport, DBQueryRow row) {
        try {
            A newReport = (A) exampleReport.getClass().newInstance();
            Field[] fields = exampleReport.getClass().getFields();
            for (Field field : fields) {
                final Object value;
                try {
                    value = field.get(exampleReport);
                    if (value != null && DBRow.class.isAssignableFrom(value.getClass())) {
                        if (value instanceof DBRow) {
                            DBRow gotDefinedRow = row.get((DBRow) value);
                            field.set(newReport, gotDefinedRow);
                        }
                    } else if (value != null && QueryableDatatype.class.isAssignableFrom(value.getClass())) {
                        if ((value instanceof QueryableDatatype) && ((QueryableDatatype) value).hasColumnExpression()) {
                            field.set(newReport, row.getExpressionColumnValue(value));
                        }
                    }
                } catch (IllegalArgumentException ex) {
                    throw new UnableToAccessDBReportFieldException(exampleReport, field, ex);
                } catch (IllegalAccessException ex) {
                    throw new UnableToAccessDBReportFieldException(exampleReport, field, ex);
                }
            }
            return newReport;
        } catch (InstantiationException ex) {
            throw new UnableToCreateDBReportSubclassException(exampleReport, ex);
        } catch (IllegalAccessException ex) {
            throw new UnableToCreateDBReportSubclassException(exampleReport, ex);
        }
    }

    private List<DBRow> getAllDBRowTemplates() {
        ArrayList<DBRow> arrayList = new ArrayList<DBRow>();
        Field[] fields = this.getClass().getFields();
        for (Field field : fields) {
            try {
                final Object fieldValue = field.get(this);
                if (DBRow.class.isAssignableFrom(fieldValue.getClass())) {
                    arrayList.add((DBRow) fieldValue);
                }
            } catch (IllegalArgumentException ex) {
                throw new UnableToAccessDBReportFieldException(this, field, ex);
            } catch (IllegalAccessException ex) {
                throw new UnableToAccessDBReportFieldException(this, field, ex);
            }
        }
        return arrayList;
    }

    private static class UnableToAccessDBReportFieldException extends RuntimeException {

        public static final long serialVersionUID = 1L;

        public UnableToAccessDBReportFieldException(Object badReport, Field field, Exception ex) {
            super("Unable To Access DBReport Field: please ensure that all DBReport fields on " + badReport.getClass().getSimpleName() + " are Public and Non-Null: Especially field: " + field.getName(), ex);
        }

        public UnableToAccessDBReportFieldException(Object badReport, Exception ex) {
            super("Unable To Access DBReport Field: please ensure that all DBReport fields on " + badReport.getClass().getSimpleName() + " are Public and Non-Null.", ex);
        }
    }

    private static class UnableToCreateDBReportSubclassException extends RuntimeException {

        public static final long serialVersionUID = 1L;

        public UnableToCreateDBReportSubclassException(Object badReport, Exception ex) {
            super("Unable To Create DBReport Instance: please ensure that your DBReport subclass, " + badReport.getClass().getSimpleName() + ", has a Public, No Parameter Constructor. The class itself may need to be \"public static\" as well.", ex);
        }
    }

}
