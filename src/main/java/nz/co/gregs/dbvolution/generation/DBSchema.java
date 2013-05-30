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
import java.util.ArrayList;
import java.util.List;
import nz.co.gregs.dbvolution.databases.DBDatabase;
import nz.co.gregs.dbvolution.databases.H2DB;

/**
 *
 * @author gregorygraham
 */
public class DBSchema {

    DBDatabase database;// = new H2DB("jdbc:h2:~/dbvolution", "", "");

    public static List<DBTableClass> generateSchemaOfTables(DBDatabase database, String packageName) throws SQLException, IllegalArgumentException, IllegalAccessException, IntrospectionException, InvocationTargetException {
        return generateSchema(database, packageName, new String[]{"TABLE"});
    }

    public static List<DBTableClass> generateSchemaOfViews(DBDatabase database, String packageName) throws SQLException, IllegalArgumentException, IllegalAccessException, IntrospectionException, InvocationTargetException {
        return generateSchema(database, packageName, new String[]{"VIEW"});
    }
    
    public static List<DBTableClass> generateSchema(DBDatabase database, String packageName, String[] dbObjectTypes) throws SQLException, IllegalArgumentException, IllegalAccessException, IntrospectionException, InvocationTargetException {
        DBSchema schema = new DBSchema();
        List<DBTableClass> dbTableClasses = new ArrayList<DBTableClass>();
        schema.database = database;
        String lineSeparator = System.getProperty("line.separator");

        Statement dbStatement = schema.database.getDBStatement();
        Connection connection = dbStatement.getConnection();
        DatabaseMetaData metaData = connection.getMetaData();
        ResultSet tables = metaData.getTables(null, null, null, dbObjectTypes);

        while (tables.next()) {
            DBTableClass dbTableClass = new DBTableClass();
            StringBuilder javaSource = new StringBuilder();
            if (packageName != null) {
                javaSource.append("package ").append(packageName).append(";");
                javaSource.append(lineSeparator);
            }
            javaSource.append("import nz.co.gregs.dbvolution.*;");
            javaSource.append(lineSeparator);
            javaSource.append("import nz.co.gregs.dbvolution.annotations.*;");
            javaSource.append(lineSeparator);
            //@DBTableName("marque") public class Marque extends DBTableRow {

            String tableName = tables.getString("TABLE_NAME");
            System.err.println(tableName);
            String className = toClassCase(tableName);
            javaSource.append("@DBTableName(\"").append(tableName).append("\") ");
            javaSource.append(lineSeparator);
            javaSource.append("public class ").append(className).append(" extends DBTableRow {");
            javaSource.append(lineSeparator);

            ResultSet columns = metaData.getColumns(null, null, tableName, null);
            while (columns.next()) {
                String columnName = columns.getString("COLUMN_NAME");
                String fieldName = toCamelCase(columnName);
                String columnType = getQueryableDatatypeOfSQLType(columns.getInt("DATA_TYPE"));
                javaSource.append("    @DBTableColumn(\"").append(columnName).append("\")");
                javaSource.append(lineSeparator);
                javaSource.append("    public ").append(columnType).append(" ").append(fieldName).append(" = new ").append(columnType).append("();");
                javaSource.append(lineSeparator);
            }
            javaSource.append("}");
            javaSource.append(lineSeparator);
            dbTableClass.className = className;
            dbTableClass.packageName = packageName;
            dbTableClass.javaSource = javaSource.toString();
            dbTableClasses.add(dbTableClass);
        }
        return dbTableClasses;
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
