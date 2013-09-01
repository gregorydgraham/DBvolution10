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
package nz.co.gregs.dbvolution.databases.definitions;

import java.sql.Types;
import java.util.Date;

import nz.co.gregs.dbvolution.datatypes.DBBoolean;
import nz.co.gregs.dbvolution.datatypes.DBByteArray;
import nz.co.gregs.dbvolution.datatypes.DBDate;
import nz.co.gregs.dbvolution.datatypes.DBInteger;
import nz.co.gregs.dbvolution.datatypes.DBNumber;
import nz.co.gregs.dbvolution.datatypes.DBObject;
import nz.co.gregs.dbvolution.datatypes.DBString;
import nz.co.gregs.dbvolution.datatypes.QueryableDatatype;

/**
 *
 * @author gregorygraham
 */
public abstract class DBDefinition {
    
    public abstract String getDateFormattedForQuery(Date date);

    public String formatColumnName(String columnName) {
        return columnName;
    }

    public String beginStringValue() {
        return "'";
    }

    public String endStringValue() {
        return "'";
    }

    public String beginNumberValue() {
        return "";
    }

    public String endNumberValue() {
        return "";
    }

    /**
     *
     * Formats the table and column name pair correctly for this database
     *
     * This should be used for column names in the select query
     *
     * e.g table, column => TABLE.COLUMN
     *
     * @param tableName
     * @param columnName
     * @return
     */
    public String formatTableAndColumnName(String tableName, String columnName) {
        return formatTableName(tableName) + "." + formatColumnName(columnName);
    }

    public String formatTableName(String tableName) {
        return tableName;
    }

    /**
     *
     * Specifies the column alias used within the JDBC ResultSet to identify the
     * column.
     *
     * @param tableName
     * @param columnName
     * @return
     */
    public String formatColumnNameForResultSet(String tableName, String columnName) {
        String formattedName = formatTableAndColumnName(tableName, columnName).replaceAll("\\.", "__");
        return ("_" + formattedName.hashCode()).replaceAll("-", "_");
    }

    public String formatTableAndColumnNameForSelectClause(String tableName, String columnName) {
        return formatTableAndColumnName(tableName, columnName) + " " + formatColumnNameForResultSet(tableName, columnName);
    }

    public String safeString(String toString) {
        return toString.replaceAll("'", "''");
    }

    /**
     *
     * returns the required SQL to begin a line within the Where Clause
     *
     * usually, but not always " and "
     *
     * @return
     */
    public String beginAndLine() {
        return " and ";
    }

    public String getDropTableStart() {
        return "DROP TABLE ";
    }

    public String getCreateTablePrimaryKeyClauseStart() {
        return ",PRIMARY KEY (";
    }

    public String getCreateTablePrimaryKeyClauseMiddle() {
        return ", ";
    }

    public String getCreateTablePrimaryKeyClauseEnd() {
        return ")";
    }

    public String getCreateTableStart() {
        return "CREATE TABLE ";
    }

    public String getCreateTableColumnsStart() {
        return "(";
    }

    public String getCreateTableColumnsSeparator() {
        return ", ";
    }

    public String getCreateTableColumnsNameAndTypeSeparator() {
        return " ";
    }

    public Object getCreateTableColumnsEnd() {
        return ")";
    }

    public String toLowerCase(String string) {
        return " lower(" + string + ")";
    }

    public String beginInsertLine() {
        return "INSERT INTO ";
    }

    public String endInsertLine() {
        return ";";
    }

    public String beginInsertColumnList() {
        return "(";
    }

    public String endInsertColumnList() {
        return ") ";
    }

    public String beginDeleteLine() {
        return "DELETE FROM ";
    }

    public String endDeleteLine() {
        return ";";
    }

    public String getEqualsComparator() {
        return " = ";
    }

    public String beginWhereClause() {
        return " WHERE ";
    }

    public String beginUpdateLine() {
        return "UPDATE ";
    }

    public String beginSetClause() {
        return " SET ";
    }

    public String getStartingSetSubClauseSeparator() {
        return "";
    }

    public String getSubsequentSetSubClauseSeparator() {
        return ",";
    }

    public String getStartingOrderByClauseSeparator() {
        return "";
    }

    public String getSubsequentOrderByClauseSeparator() {
        return ",";
    }

    public String getFalseOperation() {
        return " 1=0 ";
    }

    public String getTrueOperation() {
        return " 1=1 ";
    }

    public String getNull() {
        return " NULL ";
    }

    public String beginSelectStatement() {
        return " SELECT ";
    }

    public String beginFromClause() {
        return " FROM ";
    }

    public Object endSQLStatement() {
        return ";";
    }

    public String getStartingSelectSubClauseSeparator() {
        return "";
    }

    public String getSubsequentSelectSubClauseSeparator() {
        return ", ";
    }

    public String countStarClause() {
        return " COUNT(*) ";
    }

    /**
     *
     * @param rowLimit
     * @return
     */
    public Object getTopClause(Long rowLimit) {
        return " TOP " + rowLimit + " ";
    }

    public String beginOrderByClause() {
        return " ORDER BY ";
    }

    public String endOrderByClause() {
        return " ";
    }

    public Object getOrderByDirectionClause(Boolean sortOrder) {
        if (sortOrder == null) {
            return "";
        } else if (sortOrder) {
            return " ASC ";
        } else {
            return " DESC ";
        }
    }
    
    /**
    *
    * Returns a class of the appropriate QueryableDatatype for the specified
    * SQLType
    *
    * @param columnType
    * @return
    */
   public Class<? extends QueryableDatatype> getQueryableDatatypeOfSQLType(int columnType, int precision) {
       switch (columnType) {
           case Types.BIT:
               if (precision == 1) {
                   return DBBoolean.class;
               } else {
                   return DBByteArray.class;
               }
           case Types.INTEGER:
           case Types.BIGINT:
           case Types.BINARY:
           case Types.BOOLEAN:
           case Types.ROWID:
           case Types.SMALLINT:
               return DBInteger.class;
           case Types.DECIMAL:
           case Types.DOUBLE:
           case Types.FLOAT:
           case Types.NUMERIC:
           case Types.REAL:
               return DBNumber.class;
           case Types.VARCHAR:
           case Types.CHAR:
           case Types.NCHAR:
           case Types.NVARCHAR:
           case Types.CLOB:
           case Types.NCLOB:
           case Types.LONGNVARCHAR:
           case Types.LONGVARCHAR:
               return DBString.class;
           case Types.DATE:
           case Types.TIME:
               return DBDate.class;
           case Types.TIMESTAMP:
               return DBDate.class;
           case Types.JAVA_OBJECT:
               return DBObject.class;
           case Types.VARBINARY:
           case Types.LONGVARBINARY:
           case Types.BLOB:
               return DBByteArray.class;
           default:
               throw new IllegalArgumentException("Unknown Java SQL Type: " + columnType);
       }
   }
}
