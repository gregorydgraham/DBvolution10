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
import nz.co.gregs.dbvolution.datatypes.QueryableDatatype;

/**
 *
 * @author gregory.graham
 */
class DBReport {

    public DBReport() {
        super();
    }

    public static <A extends DBReport> List<A> getAllRows(DBDatabase database, A exampleReport) throws SQLException {
        List<A> reportRows = new ArrayList<A>();
        DBQuery query = database.getDBQuery();
        addTablesAndExpressions(query, exampleReport);
        query.setBlankQueryAllowed(true);
        List<DBQueryRow> allRows = query.getAllRows();
        for (DBQueryRow row : allRows) {
            reportRows.add(DBReport.getReportInstance(exampleReport, row));
        }
        return reportRows;
    }

    public static <A extends DBReport> List<A> getRows(DBDatabase database, A exampleReport, DBRow... rows) throws SQLException {
        List<A> reportRows = new ArrayList<A>();
        DBQuery query = database.getDBQuery();
        addTablesAndExpressions(query, exampleReport);
//        query.addExtraCriteria(rows);
        List<DBQueryRow> allRows = query.getAllRows();
        for (DBQueryRow row : allRows) {
            reportRows.add(DBReport.getReportInstance(exampleReport, row));
        }
        return reportRows;
    }

    private static <A extends DBReport> void addTablesAndExpressions(DBQuery query, A exampleReport) {
//        List<DBRow> tables = new ArrayList<DBRow>();
        Field[] fields = exampleReport.getClass().getFields();
        if (fields.length == 0) {
            throw new UnableToAccessDBReportFieldException(exampleReport, null);
        }
        for (Field field : fields) {
            final Object value;
            try {
                value = field.get(exampleReport);
                if (value != null && DBRow.class.isAssignableFrom(value.getClass())) {
                    if (value instanceof DBRow) {
                        query.add((DBRow) value);
                    }
                } else if (value != null && QueryableDatatype.class.isAssignableFrom(value.getClass())) {
                    if ((value instanceof QueryableDatatype) && ((QueryableDatatype) value).hasColumnExpression()) {
                        query.addExpressionColumn(value, ((QueryableDatatype) value).getColumnExpression());
                    }
                }
            } catch (IllegalArgumentException ex) {
                throw new UnableToAccessDBReportFieldException(exampleReport, ex);
            } catch (IllegalAccessException ex) {
                throw new UnableToAccessDBReportFieldException(exampleReport, ex);
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
                    throw new UnableToAccessDBReportFieldException(exampleReport, ex);
                } catch (IllegalAccessException ex) {
                    throw new UnableToAccessDBReportFieldException(exampleReport, ex);
                }
            }
            return newReport;
        } catch (InstantiationException ex) {
            throw new UnableToCreateDBReportSubclassException(exampleReport, ex);
        } catch (IllegalAccessException ex) {
            throw new UnableToCreateDBReportSubclassException(exampleReport, ex);
        }
    }

    private static class UnableToAccessDBReportFieldException extends RuntimeException {

        public static final long serialVersionUID = 1L;

        public UnableToAccessDBReportFieldException(Object badReport, Exception ex) {
            super("Unable To Access DBReport Field: please ensure that all DBReport fields on " + badReport.getClass().getSimpleName() + " are Public and Non-Null", ex);
        }
    }

    private static class UnableToCreateDBReportSubclassException extends RuntimeException {

        public static final long serialVersionUID = 1L;

        public UnableToCreateDBReportSubclassException(Object badReport, Exception ex) {
            super("Unable To Create DBReport Instance: please ensure that your DBReport subclass, " + badReport.getClass().getSimpleName() + " has a Public, No Parameter Constructor.", ex);
        }
    }

}
