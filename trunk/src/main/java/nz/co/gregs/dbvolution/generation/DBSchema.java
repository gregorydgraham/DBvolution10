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
package nz.co.gregs.dbvolution.generation;

import java.beans.IntrospectionException;
import java.lang.reflect.InvocationTargetException;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Types;
import nz.co.gregs.dbvolution.databases.DBDatabase;
import nz.co.gregs.dbvolution.databases.H2DB;

/**
 *
 * @author gregorygraham
 */
public class DBSchema {

    DBDatabase database = new H2DB("jdbc:h2:~/dbvolution", "", "");

    public static void generateSchema(DBDatabase database, String packageName) throws SQLException, IllegalArgumentException, IllegalAccessException, IntrospectionException, InvocationTargetException {
        DBSchema schema = new DBSchema();
        schema.database = database;

        Statement dbStatement = schema.database.getDBStatement();
        Connection connection = dbStatement.getConnection();
        DatabaseMetaData metaData = connection.getMetaData();
        ResultSet tables = metaData.getTables(null, null, null, new String[]{"TABLE"});

        while (tables.next()) {
            if (packageName != null) {
                System.out.println("package " + packageName + ";");
                System.out.println("");
            }
            System.out.println("import nz.co.gregs.dbvolution.*;");
            System.out.println("import nz.co.gregs.dbvolution.annotations.*;");
            System.out.println();
            //@DBTableName("marque") public class Marque extends DBTableRow {

            String tableName = tables.getString("TABLE_NAME");
            String className = toClassCase(tableName);
            System.out.println("@DBTableName(\"" + tableName + "\") ");
            System.out.println("public class " + className + " extends DBTableRow {");
            System.out.println();

            ResultSet columns = metaData.getColumns(null, null, tableName, null);
            while (columns.next()) {
                String columnName = columns.getString("COLUMN_NAME");
                String fieldName = toCamelCase(columnName);
                String columnType = getQueryableDatatypeOfSQLType(columns.getInt("DATA_TYPE"));
                System.out.println("    @DBTableColumn(\"" + columnName + "\")");
                System.out.println("    public " + columnType + " " + fieldName + " = new " + columnType + "();");
            System.out.println("");
            }
            System.out.println("}");
        }
    }

    private static String getQueryableDatatypeOfSQLType(int columnType) {
        String value = "";
        switch (columnType) {
            case Types.INTEGER:
            case Types.BIGINT:
            case Types.BINARY:
            case Types.BOOLEAN:
            case Types.ROWID:
            case Types.SMALLINT:
                value = "DBInteger";
                break;
            case Types.DECIMAL:
            case Types.DOUBLE:
            case Types.FLOAT:
            case Types.NUMERIC:
            case Types.REAL:
                value = "DBNumber";
                break;
            case Types.VARCHAR:
            case Types.CHAR:
            case Types.NCHAR:
            case Types.NVARCHAR:
            case Types.CLOB:
            case Types.NCLOB:
            case Types.LONGNVARCHAR:
            case Types.LONGVARCHAR:
                value = "DBString";
                break;
            case Types.DATE:
            case Types.TIME:
                value = "DBDate";
                break;
            case Types.TIMESTAMP:
                value = "DBDate";
                break;
            case Types.VARBINARY:
            case Types.JAVA_OBJECT:
            case Types.LONGVARBINARY:
                value = "DBBlob";
                break;
            default:
                throw new RuntimeException("Unknown Java SQL Type: " + columnType);
        }
        return value;
    }

    static String toClassCase(String s) {
        String[] parts = s.split("_");
        String camelCaseString = "";
        for (String part : parts) {
            camelCaseString = camelCaseString + toProperCase(part);
        }
        return camelCaseString;
    }

    static String toCamelCase(String s) {
        String[] parts = s.split("_");
        String camelCaseString = "";
        for (String part : parts) {
            camelCaseString = camelCaseString + toProperCase(part);
        }
        camelCaseString = camelCaseString.substring(0, 1).toLowerCase() + camelCaseString.substring(1);
        return camelCaseString;
    }

    static String toProperCase(String s) {
        return s.substring(0, 1).toUpperCase()
                + s.substring(1).toLowerCase();
    }
}
