package nz.co.gregs.dbvolution;

import java.io.PrintStream;
import java.lang.reflect.Field;
import java.sql.*;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import nz.co.gregs.dbvolution.annotations.DBSelectQuery;
import nz.co.gregs.dbvolution.annotations.DBColumn;
import nz.co.gregs.dbvolution.annotations.DBPrimaryKey;
import nz.co.gregs.dbvolution.databases.DBDatabase;

/**
 *
 * @param <E>
 * @author gregory.graham
 */
public class DBTable<E extends DBRow> {

    private static final long serialVersionUID = 1L;
    private static boolean printSQLBeforeExecuting = false;

    /**
     *
     * @param <T>
     * @return
     */
    public static <E extends DBRow> DBTable<E> getInstance(DBDatabase database, E example) {
        DBTable<E> dbTable = new DBTable<E>(database, example);
        return dbTable;
    }
    private DBDatabase theDatabase = null;
    E dummy;
    private java.util.ArrayList<E> listOfRows = new java.util.ArrayList<E>();
    private Long rowLimit;
    private QueryableDatatype[] sortOrder = null;
    private E sortBase;

    /**
     *
     *
     * @param myDatabase
     * @param dummyObject
     */
    private DBTable(DBDatabase myDatabase, E dummyObject) {
        this.theDatabase = myDatabase;
        dummy = dummyObject;
    }

    /**
     * Set this to TRUE to see the actual SQL that is executed.
     *
     * @param aPrintSQLBeforeExecuting the printSQLBeforeExecuting to set
     */
    public static void setPrintSQLBeforeExecuting(boolean aPrintSQLBeforeExecuting) {
        printSQLBeforeExecuting = aPrintSQLBeforeExecuting;
    }

    private String getDBColumnName(Field field) {
        String columnName = "";

        if (field.isAnnotationPresent(DBColumn.class)) {
            DBColumn annotation = field.getAnnotation(DBColumn.class);
            columnName = annotation.value();
            if (columnName
                    == null || columnName.isEmpty()) {
                columnName = field.getName();
            }
        }
        return columnName;
    }

    private String getAllFieldsForSelect() {
        StringBuilder allFields = new StringBuilder();
        @SuppressWarnings("unchecked")
        Class<E> thisClass = (Class<E>) dummy.getClass();
        Field[] fields = thisClass.getDeclaredFields();
        String separator = "";
        for (Field field : fields) {
            if (field.isAnnotationPresent(DBColumn.class)) {
                allFields.append(separator).append(" ").append(getDBColumnName(field));
                separator = ",";
            }
        }
        return allFields.toString();
    }

    private String getSQLForSelectAll() {
        StringBuilder selectStatement = new StringBuilder();
        DBSelectQuery selectQueryAnnotation = dummy.getClass().getAnnotation(DBSelectQuery.class);
        if (selectQueryAnnotation != null) {
            selectStatement.append(selectQueryAnnotation.value());
        } else {
            selectStatement.append(theDatabase.beginSelectStatement());
            if (rowLimit != null) {
                selectStatement.append(theDatabase.getTopClause(rowLimit));
            }
            selectStatement.append(getAllFieldsForSelect())
                    .append(" from ")
                    .append(dummy.getTableName())
                    .append(getOrderByClause())
                    .append(";");
        }

        return selectStatement.toString();
    }

    /**
     *
     * Retruns the
     *
     * @return
     */
    public String getSQLForSelect() {
        StringBuilder selectStatement = new StringBuilder();
        DBSelectQuery selectQueryAnnotation = dummy.getClass().getAnnotation(DBSelectQuery.class);
        if (selectQueryAnnotation != null) {
            selectStatement
                    .append(selectQueryAnnotation.value())
                    .append(theDatabase.beginWhereClause())
                    .append(theDatabase.getTrueOperation());
        } else {
            selectStatement.append(theDatabase.beginSelectStatement());
            if (rowLimit != null) {
                selectStatement.append(theDatabase.getTopClause(rowLimit));
            }

            selectStatement
                    .append(getAllFieldsForSelect())
                    .append(theDatabase.beginFromClause())
                    .append(dummy.getTableName())
                    .append(theDatabase.beginWhereClause())
                    .append(theDatabase.getTrueOperation());
        }
        return selectStatement.toString();
    }

    /**
     * Use this carefully as it does what it says on the label: Gets All Rows of
     * the table from the database.
     *
     * If your database has umpteen gazillion rows in VeryBig table and you call
     * this, don't come crying to me.
     *
     * @throws SQLException
     */
    public DBTable<E> getAllRows() throws SQLException {
        this.listOfRows.clear();

        String selectStatement = this.getSQLForSelectAll();

        if (printSQLBeforeExecuting || theDatabase.isPrintSQLBeforeExecuting()) {
            System.out.println(selectStatement);
        }

        Statement statement;
        ResultSet resultSet;
        statement = this.theDatabase.getDBStatement();
        try {
            boolean executed = statement.execute(selectStatement);
        } catch (SQLException noConnection) {
            throw new RuntimeException("Unable to create a Statement: please check the database URL, username, and password, and that the appropriate libaries have been supplied: URL=" + theDatabase.getJdbcURL() + " USERNAME=" + theDatabase.getUsername(), noConnection);
        }
        try {
            resultSet = statement.getResultSet();
        } catch (SQLException noConnection) {
            throw new RuntimeException("Unable to create a Statement: please check the database URL, username, and password, and that the appropriate libaries have been supplied: URL=" + theDatabase.getJdbcURL() + " USERNAME=" + theDatabase.getUsername(), noConnection);
        }
        addAllFields(this, resultSet);
        return this;
    }

    private void addAllFields(DBTable<E> dbTable, ResultSet resultSet) throws SQLException {
        ResultSetMetaData rsMeta = resultSet.getMetaData();
        Map<String, Integer> dbColumnNames = new HashMap<String, Integer>();
        for (int k = 1; k <= rsMeta.getColumnCount(); k++) {
            dbColumnNames.put(rsMeta.getColumnName(k), k);
        }

        while (resultSet.next()) {
            @SuppressWarnings("unchecked")
            E tableRow = (E) DBRow.getDBRow(dummy.getClass());

            Field[] fields = tableRow.getClass().getDeclaredFields();



            for (Field field : fields) {
                if (field.isAnnotationPresent(DBColumn.class)) {
                    String dbColumnName = getDBColumnName(field);
                    int dbColumnIndex = dbColumnNames.get(theDatabase.formatColumnName(dbColumnName));

                    setObjectFieldValueToColumnValue(rsMeta, dbColumnIndex, field, tableRow, resultSet, dbColumnName);
                }
            }
            dbTable.listOfRows.add(tableRow);
        }
    }

    private void setObjectFieldValueToColumnValue(ResultSetMetaData rsMeta, int dbColumnIndex, Field field, DBRow tableRow, ResultSet resultSet, String dbColumnName) throws SQLException {
        QueryableDatatype qdt = tableRow.getQueryableValueOfField(field);
        int columnType = rsMeta.getColumnType(dbColumnIndex);
        switch (columnType) {
            case Types.INTEGER:
            case Types.BIGINT:
            case Types.BINARY:
            case Types.BOOLEAN:
            case Types.ROWID:
            case Types.SMALLINT:
                Long aLong = resultSet.getLong(dbColumnName);
                if (resultSet.wasNull()) {
                    qdt.isNull();
                } else {
                    qdt.isLiterally(aLong);
                }
                break;
            case Types.DECIMAL:
            case Types.DOUBLE:
            case Types.FLOAT:
            case Types.NUMERIC:
            case Types.REAL:
                Double aDouble = resultSet.getDouble(dbColumnName);
                if (resultSet.wasNull()) {
                    qdt.isNull();
                } else {
                    qdt.isLiterally(aDouble);
                }
                break;
            case Types.VARCHAR:
            case Types.CHAR:
            case Types.NCHAR:
            case Types.NVARCHAR:
            case Types.CLOB:
            case Types.NCLOB:
            case Types.LONGNVARCHAR:
            case Types.LONGVARCHAR:
                String string = resultSet.getString(dbColumnName);
                if (resultSet.wasNull()) {
                    qdt.isNull();
                } else {
                    qdt.isLiterally(string);
                }
                break;
            case Types.DATE:
            case Types.TIME:
                Date date = resultSet.getDate(dbColumnName);
                if (resultSet.wasNull()) {
                    qdt.isNull();
                } else {
                    qdt.isLiterally(date);
                }
                break;
            case Types.TIMESTAMP:
                Timestamp timestamp = resultSet.getTimestamp(dbColumnName);
                if (resultSet.wasNull()) {
                    qdt.isNull();
                } else {
                    qdt.isLiterally(timestamp);
                }
                break;
            case Types.VARBINARY:
            case Types.JAVA_OBJECT:
            case Types.LONGVARBINARY:
                Object obj = resultSet.getObject(dbColumnName);
                if (resultSet.wasNull()) {
                    qdt.isNull();
                } else {
                    qdt.isLiterally(obj);
                }
                break;
            default:
                throw new RuntimeException("Unknown Java SQL Type: " + rsMeta.getColumnType(dbColumnIndex));
        }
    }

    private String getPrimaryKeyColumn() {
        String pkColumn = "";
        @SuppressWarnings("unchecked")
        Class<E> thisClass = (Class<E>) dummy.getClass();
        Field[] fields = thisClass.getDeclaredFields();
        for (Field field : fields) {
            if (field.isAnnotationPresent(DBPrimaryKey.class)) {
                pkColumn = this.getDBColumnName(field);
            }
        }
        if (pkColumn.isEmpty()) {
            throw new RuntimeException("Primary Key Field Not Defined: Please define the primary key field of " + thisClass.getSimpleName() + " using the @DBPrimaryKey annotation.");
        } else {
            return pkColumn;
        }
    }

    private String escapeSingleQuotes(String str) {
        if (str == null) {
            return "";
        }
        return str.replace("'", "''").replace("\\", "\\\\");
    }

    private DBTable<E> getRows(String whereClause) throws SQLException {
        this.listOfRows.clear();
        String selectStatement = this.getSQLForSelect() + whereClause + getOrderByClause() + ";";
        if (printSQLBeforeExecuting || theDatabase.isPrintSQLBeforeExecuting()) {
            System.out.println(selectStatement);
        }

        Statement statement = theDatabase.getDBStatement();
        boolean executed = statement.execute(selectStatement);
        ResultSet resultSet = statement.getResultSet();

        addAllFields(this, resultSet);
        return this;
    }

    /**
     * Retrieves the row (or rows in a bad database) that has the specified
     * primary key The primary key column is identified by the
     *
     * @DBPrimaryKey annotation in the TableRow subclass
     *
     * @param pkValue
     * @return
     * @throws SQLException
     */
    public DBTable<E> getRowsByPrimaryKey(Object pkValue) throws SQLException {
        String whereClause = " and " + getPrimaryKeyColumn() + " = '" + escapeSingleQuotes(pkValue.toString()) + "'";
        this.getRows(whereClause);
        return this;
    }

    public DBTable<E> getRowsByPrimaryKey(Number pkValue) throws SQLException {
        String whereClause = " and " + getPrimaryKeyColumn() + " = " + pkValue + " ";
        this.getRows(whereClause);
        return this;
    }

    public DBTable<E> getRowsByPrimaryKey(Date pkValue) throws SQLException {
        String whereClause = " and " + getPrimaryKeyColumn() + " = " + this.theDatabase.getDateFormattedForQuery(pkValue) + " ";
        this.getRows(whereClause);
        return this;
    }

    /**
     * Using TableRow subclass as an example this method retrieves all the
     * appropriate records The following will retrieve all records from the
     * table where the Language column contains JAVA MyTableRow myExample = new
     * MyTableRow(); myExample.getLanguage.useLikeComparison("%JAVA%"); (new
     * DBTable<MyTableRow>()).getByExample(myExample);
     *
     * All columns defined within the TableRow subclass as QueryableDatatype
     * (e.g. DBNumber, DBString, etc) can be used in this way N.B. an actual
     *
     * @param queryTemplate
     * @return
     * @throws SQLException
     */
    public DBTable<E> getRowsByExample(E queryTemplate) throws SQLException {
        return getRows(getSQLForExample(queryTemplate));
    }

    public E getOnlyRowByExample(E queryTemplate) throws SQLException, UnexpectedNumberOfRowsException {
        return getRowsByExample(queryTemplate, 1).listOfRows.get(0);
    }

    public DBTable<E> getRowsByExample(E queryTemplate, int expectedNumberOfRows) throws SQLException, UnexpectedNumberOfRowsException {
        DBTable<E> rowsByExample = getRowsByExample(queryTemplate);
        int actualNumberOfRows = rowsByExample.toList().size();
        if (actualNumberOfRows == expectedNumberOfRows) {
            return rowsByExample;
        } else {
            throw new UnexpectedNumberOfRowsException(expectedNumberOfRows, actualNumberOfRows, "Unexpected Number Of Rows Detected: was expecting "
                    + expectedNumberOfRows
                    + ", found "
                    + actualNumberOfRows);
        }
    }

    /**
     * Returns the WHERE clause used by the getByExample method. Provided to aid
     * understanding and debugging.
     *
     * @param query
     * @return
     */
    public String getSQLForExample(E query) {
        return query.getWhereClause(theDatabase);
    }

    /**
     * For the particularly hard queries, just provide the actual WHERE clause
     * you want to use.
     *
     * myExample.getLanguage.isLike("%JAVA%"); is similar to: getByRawSQL("and
     * language like '%JAVA%'");
     *
     * N.B. the starting AND is optional and avoid trailing semicolons
     *
     * @param sqlWhereClause
     * @return
     */
    public DBTable<E> getRowsByRawSQL(String sqlWhereClause) throws SQLException {
        if (sqlWhereClause.toLowerCase().matches("^\\s*and\\s+.*")) {
            return getRows(sqlWhereClause.replaceAll("\\s*;\\s*$", ""));
        } else {
            return getRows(" AND " + sqlWhereClause.replaceAll("\\s*;\\s*$", ""));
        }
    }

    /**
     * Convenience method to print all the rows in the current collection
     * Equivalent to: print(System.out)
     *
     */
    public void print() {
        print(System.out);
    }

    /**
     * the same as print() but allows you to specify the PrintStream required
     *
     * myTable.printAllRows(System.err);
     *
     * @param ps
     */
    public void print(PrintStream ps) {
        for (E row : this.listOfRows) {
            ps.println(row);
        }
    }

    /**
     *
     * Returns the first row of the table, particularly helpful when you know
     * there is only one row
     *
     * @return
     */
    public E getFirstRow() {
        if (this.listOfRows.size() > 0) {
            return this.listOfRows.get(0);
        } else {
            return null;
        }
    }

    public E getOnlyRow() throws UnexpectedNumberOfRowsException {
        if (this.listOfRows.size() > 0) {
            return this.listOfRows.get(0);
        } else {
            throw new UnexpectedNumberOfRowsException(1, listOfRows.size(), "Unexpected Number Of Rows Detected: was expecting 1, found " + listOfRows.size());
        }
    }

    public void insert(E newRow) throws SQLException {
        ArrayList<E> arrayList = new ArrayList<E>();
        arrayList.add(newRow);
        insert(arrayList);
    }

    /**
     *
     * @param newRows
     * @throws SQLException
     */
    public void insert(List<E> newRows) throws SQLException {
        Statement statement = theDatabase.getDBStatement();
        List<String> allInserts = getSQLForInsert(newRows);
        for (String sql : allInserts) {
            if (printSQLBeforeExecuting || theDatabase.isPrintSQLBeforeExecuting()) {
                System.out.println(sql);
            }
            statement.addBatch(sql);
        }
        statement.executeBatch();
    }

    /**
     *
     * returns the SQL that will be used to insert the row.
     *
     * Useful for debugging and reversion scripts
     *
     * @param newRow
     * @return
     */
    public String getSQLForInsert(E newRow) {
        ArrayList<E> arrayList = new ArrayList<E>();
        arrayList.add(newRow);
        return getSQLForInsert(arrayList).get(0);
    }

    /**
     *
     * returns the SQL that will be used to insert the rows.
     *
     * Useful for debugging and reversion scripts
     *
     * @param newRows
     * @return
     */
    public List<String> getSQLForInsert(List<E> newRows) {
        List<String> allInserts = new ArrayList<String>();
        for (E row : newRows) {
            String sql =
                    theDatabase.beginInsertLine()
                    + row.getTableName()
                    + theDatabase.beginInsertColumnList()
                    + this.getAllFieldsForSelect()
                    + theDatabase.endInsertColumnList()
                    + row.getValuesClause(theDatabase)
                    + theDatabase.endInsertLine();
            allInserts.add(sql);
        }
        return allInserts;
    }

    public void delete(E oldRow) throws SQLException {
        ArrayList<E> arrayList = new ArrayList<E>();
        arrayList.add(oldRow);
        delete(arrayList);
    }

    /**
     *
     * @param oldRows
     * @throws SQLException
     */
    public void delete(List<E> oldRows) throws SQLException {
        Statement statement = theDatabase.getDBStatement();
        List<String> allSQL = getSQLForDelete(oldRows);
        for (String sql : allSQL) {
            if (printSQLBeforeExecuting || theDatabase.isPrintSQLBeforeExecuting()) {
                System.out.println(sql);
            }
            statement.addBatch(sql);
        }
        statement.executeBatch();
    }

    /**
     *
     * Convenience method
     *
     * @param oldRow
     * @return
     */
    public String getSQLForDelete(E oldRow) {
        ArrayList<E> arrayList = new ArrayList<E>();
        arrayList.add(oldRow);
        return getSQLForDelete(arrayList).get(0);
    }

    /**
     *
     * @param oldRows
     * @return
     */
    public List<String> getSQLForDelete(List<E> oldRows) {
        List<String> allInserts = new ArrayList<String>();
        for (E row : oldRows) {
//            row.setDatabase(theDatabase);
            String sql =
                    theDatabase.beginDeleteLine()
                    + row.getTableName()
                    + theDatabase.beginWhereClause()
                    + this.getPrimaryKeyColumn()
                    + theDatabase.getEqualsComparator()
                    + row.getPrimaryKeySQLStringValue(theDatabase)
                    + theDatabase.endDeleteLine();
            allInserts.add(sql);
        }
        return allInserts;
    }

    /**
     *
     * @param oldRow
     * @return
     */
    public String getSQLForDeleteWithoutPrimaryKey(E oldRow) {
        List<E> rows = new ArrayList<E>();
        rows.add(oldRow);
        return getSQLForDeleteWithoutPrimaryKey(rows).get(0);
    }

    /**
     *
     * @param oldRows
     * @return
     */
    public List<String> getSQLForDeleteWithoutPrimaryKey(List<E> oldRows) {
        List<String> allInserts = new ArrayList<String>();
        for (E row : oldRows) {
            String sql =
                    theDatabase.beginDeleteLine()
                    + row.getTableName()
                    + theDatabase.beginWhereClause();
            for (QueryableDatatype qdt : row.getQueryableDatatypes()) {
                sql = sql
                        + theDatabase.beginAndLine()
                        + row.getDBColumnName(qdt)
                        + theDatabase.getEqualsComparator()
                        + qdt.getSQLValue();
            }
            sql = sql + theDatabase.endDeleteLine();
            allInserts.add(sql);
        }
        return allInserts;
    }

    public void update(E oldRow) throws SQLException {
        ArrayList<E> arrayList = new ArrayList<E>();
        arrayList.add(oldRow);
        update(arrayList);
    }

    public void update(List<E> oldRows) throws SQLException {
        Statement statement = theDatabase.getDBStatement();
        List<String> allSQL = getSQLForUpdate(oldRows);
        for (String sql : allSQL) {
            if (printSQLBeforeExecuting || theDatabase.isPrintSQLBeforeExecuting()) {
                System.out.println(sql);
            }
            statement.addBatch(sql);
        }
        statement.executeBatch();
        for (DBRow row : oldRows) {
            row.getPrimaryKeyQueryableDatatype(theDatabase).setUnchanged();
        }
    }

    /**
     *
     * Convenience method for getSQLForUpdate(List<E>)
     *
     * @param oldRow
     * @return
     */
    public String getSQLForUpdate(E oldRow) {
        ArrayList<E> arrayList = new ArrayList<E>();
        arrayList.add(oldRow);
        return getSQLForUpdate(arrayList).get(0);
    }

    /**
     * Creates the SQL used to update the rows.
     *
     * Helpful for debugging and reversion scripts
     *
     *
     * @param oldRows
     * @return
     */
    public List<String> getSQLForUpdate(List<E> oldRows) {
        List<String> allSQL = new ArrayList<String>();
        for (E row : oldRows) {
            String sql =
                    theDatabase.beginUpdateLine()
                    + theDatabase.formatTableName(row.getTableName())
                    + theDatabase.beginSetClause();
            sql = sql + row.getSetClause(theDatabase);
            sql = sql + theDatabase.beginWhereClause()
                    + theDatabase.formatColumnName(this.getPrimaryKeyColumn())
                    + theDatabase.getEqualsComparator()
                    + row.getPrimaryKeySQLStringValue(theDatabase)
                    + theDatabase.endDeleteLine();
            allSQL.add(sql);
        }

        return allSQL;
    }

    /**
     *
     * @param query
     * @param sqlWhereClause
     * @return
     */
    public String getWhereClauseWithExampleAndRawSQL(E query, String sqlWhereClause) {
        if (sqlWhereClause.toLowerCase().matches("^\\s*and\\s+.*")) {
            return getSQLForExample(query) + sqlWhereClause.replaceAll("\\s*;\\s*$", "");
        } else {
            return getSQLForExample(query) + " AND " + sqlWhereClause.replaceAll("\\s*;\\s*$", "");
        }
    }

    /**
     *
     * @return
     */
    public List<E> toList() {
        return new java.util.ArrayList<E>(listOfRows);
    }

    public List<Number> getPrimaryKeysAsNumber() {
        List<Number> primaryKeys = new ArrayList<Number>();
        for (E e : listOfRows) {
            primaryKeys.add(e.getPrimaryKeyLongValue());
        }
        return primaryKeys;
    }

    public List<String> getPrimaryKeysAsString() {
        List<String> primaryKeys = new ArrayList<String>();
        for (E e : listOfRows) {
            primaryKeys.add(e.getPrimaryKeyStringValue());
        }
        return primaryKeys;
    }

    /**
     * Compares 2 tables, presumably from different databases
     *
     * Should be updated to return the varying rows somehow
     *
     * @param secondTable
     */
    public void compare(DBTable<E> secondTable) {
        HashMap<Long, E> secondMap = new HashMap<Long, E>();
        for (E row : secondTable.toList()) {
            secondMap.put(row.getPrimaryKeyLongValue(), row);
        }
        for (E row : this.toList()) {
            E foundRow = secondMap.get(row.getPrimaryKeyLongValue());
            if (foundRow == null) {
                System.out.println("NOT FOUND: " + row);
            } else if (!row.toString().equals(foundRow.toString())) {
                System.out.println("DIFFERENT: " + row);
                System.out.println("         : " + foundRow);
            }
        }
    }

    public void setRowLimit(int i) {
        rowLimit = new Long(i);
    }

    public void clearRowLimit() {
        rowLimit = null;
    }

    public void setSortOrder(E baseRow, QueryableDatatype... orderColumns) {
        sortBase = baseRow;
        sortOrder = orderColumns;
    }

    public void clearSortOrder() {
        sortOrder = null;
    }

    private String getOrderByClause() {
        if (sortOrder != null) {
            StringBuilder orderByClause = new StringBuilder(theDatabase.beginOrderByClause());
            String sortSeparator = theDatabase.getStartingOrderByClauseSeparator();
            for (QueryableDatatype qdt : sortOrder) {
                final String dbColumnName = sortBase.getDBColumnName(qdt);
                if (dbColumnName != null) {
                    orderByClause.append(sortSeparator).append(dbColumnName).append(theDatabase.getOrderByDirectionClause(qdt.getSortOrder()));
                    sortSeparator = theDatabase.getSubsequentOrderByClauseSeparator();
                }
            }
            orderByClause.append(theDatabase.endOrderByClause());
            return orderByClause.toString();
        }
        return "";
    }
}
