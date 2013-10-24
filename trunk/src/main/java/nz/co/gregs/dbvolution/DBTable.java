package nz.co.gregs.dbvolution;

import nz.co.gregs.dbvolution.datatypes.QueryableDatatype;
import nz.co.gregs.dbvolution.datatypes.DBLargeObject;
import nz.co.gregs.dbvolution.exceptions.*;
import java.io.PrintStream;
import java.lang.reflect.Field;
import java.sql.*;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import nz.co.gregs.dbvolution.actions.DBAction;
import nz.co.gregs.dbvolution.actions.DBActionList;
import nz.co.gregs.dbvolution.actions.DBSave;
import nz.co.gregs.dbvolution.annotations.DBSelectQuery;
import nz.co.gregs.dbvolution.annotations.DBColumn;
import nz.co.gregs.dbvolution.annotations.DBPrimaryKey;
import nz.co.gregs.dbvolution.databases.definitions.DBDefinition;
import nz.co.gregs.dbvolution.exceptions.UndefinedPrimaryKeyException;
import nz.co.gregs.dbvolution.internal.PropertyWrapper;
import nz.co.gregs.dbvolution.operators.DBIsNullOperator;

/**
 *
 * @param <E>
 * @author gregory.graham
 */
public class DBTable<E extends DBRow> {

    private static final long serialVersionUID = 1L;
    private static boolean printSQLBeforeExecuting = false;
    E dummy;
    private DBDatabase database = null;
    private java.util.ArrayList<E> listOfRows = new java.util.ArrayList<E>();
    private Long rowLimit;
    private QueryableDatatype[] sortOrder = null;
    private E sortBase;

    /**
     *
     * @param <E>
     * @param database
     * @param example
     * @return
     */
    public static <E extends DBRow> DBTable<E> getInstance(DBDatabase database, E example) {
        DBTable<E> dbTable = new DBTable<E>(database, example);
        return dbTable;
    }

    /**
     *
     *
     * @param myDatabase
     * @param dummyObject
     */
    private DBTable(DBDatabase myDatabase, E dummyObject) {
        this.database = myDatabase;
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
            if (columnName == null || columnName.isEmpty()) {
                columnName = field.getName();
            }
        }
        return columnName;
    }

    private String getAllFieldsForSelect() {
        StringBuilder allFields = new StringBuilder();
        @SuppressWarnings("unchecked")
        List<String> columnNames = dummy.getColumnNames();
        String separator = "";
        for (String column : columnNames) {
            allFields.append(separator).append(" ").append(database.getDefinition().formatColumnName(column));
            separator = ",";
        }

        return allFields.toString();
    }

    private String getAllFieldsForInsert() {
        StringBuilder allFields = new StringBuilder();
        DBDefinition defn = database.getDefinition();
        List<PropertyWrapper> props = dummy.getPropertyWrappers();
        String separator = "";
        for (PropertyWrapper prop : props) {
            // BLOBS are not inserted.so exclude them
            if (prop.isColumn() && !(prop.getQueryableDatatype() instanceof DBLargeObject)) {
                allFields
                        .append(separator)
                        .append(" ")
                        .append(defn.formatColumnName(prop.columnName()));
                separator = ",";
            }
        }
        return allFields.toString();
    }

    private String getSQLForSelectAll() {
        DBDefinition defn = database.getDefinition();
        StringBuilder selectStatement = new StringBuilder();
        DBSelectQuery selectQueryAnnotation = dummy.getClass().getAnnotation(DBSelectQuery.class);
        if (selectQueryAnnotation
                != null) {
            selectStatement.append(selectQueryAnnotation.value());
        } else {
            selectStatement.append(defn.beginSelectStatement());
            if (rowLimit != null) {
                selectStatement.append(defn.getLimitRowsSubClauseDuringSelectClause(rowLimit));
            }
            selectStatement.append(getAllFieldsForSelect())
                    .append(" from ")
                    .append(defn.formatTableName(dummy.getTableName()))
                    .append(getOrderByClause())
                    .append(defn.getLimitRowsSubClauseAfterWhereClause(rowLimit))
                    .append(defn.endSQLStatement());
        }

        return selectStatement.toString();
    }

    /**
     *
     * Returns the
     *
     * @return
     */
    public String getSQLForSelect() {
        DBDefinition defn = database.getDefinition();
        StringBuilder selectStatement = new StringBuilder();
        DBSelectQuery selectQueryAnnotation = dummy.getClass().getAnnotation(DBSelectQuery.class);
        if (selectQueryAnnotation != null) {
            selectStatement
                    .append(selectQueryAnnotation.value())
                    .append(defn.beginWhereClause())
                    .append(defn.getTrueOperation());
        } else {
            selectStatement.append(defn.beginSelectStatement());
            if (rowLimit != null) {
                selectStatement.append(defn.getLimitRowsSubClauseDuringSelectClause(rowLimit));
            }

            selectStatement
                    .append(getAllFieldsForSelect())
                    .append(defn.beginFromClause())
                    .append(defn.formatTableName(dummy.getTableName()))
                    .append(defn.beginWhereClause())
                    .append(defn.getTrueOperation());
        }

        return selectStatement.toString();
    }

    /**
     * Gets All Rows of the table from the database
     *
     * Use this carefully as it does what it says on the label: Gets All Rows of
     * the table from the database.
     *
     * throws AccidentalBlankQueryException if you haven't specifically allowed
     * blank queries with setBlankQueryAllowed(boolean)
     *
     * @throws SQLException, AccidentalBlankQueryException
     */
    public DBTable<E> getAllRows() throws SQLException {
        this.listOfRows.clear();

        String selectStatement = this.getSQLForSelectAll();

        if (printSQLBeforeExecuting || database.isPrintSQLBeforeExecuting()) {
            System.out.println(selectStatement);
        }

        Statement statement;
        ResultSet resultSet;
        statement = this.database.getDBStatement();
        try {
            boolean executed = statement.execute(selectStatement);
        } catch (SQLException noConnection) {
            throw new RuntimeException("Unable to create a Statement: please check the database URL, username, and password, and that the appropriate libaries have been supplied: URL=" + database.getJdbcURL() + " USERNAME=" + database.getUsername(), noConnection);
        }
        try {
            resultSet = statement.getResultSet();
        } catch (SQLException noConnection) {
            throw new RuntimeException("Unable to create a Statement: please check the database URL, username, and password, and that the appropriate libaries have been supplied: URL=" + database.getJdbcURL() + " USERNAME=" + database.getUsername(), noConnection);
        }
        addAllFields(this, resultSet);
        return this;
    }

    private void addAllFields(DBTable<E> dbTable, ResultSet resultSet) throws SQLException {
        DBDefinition defn = database.getDefinition();
        ResultSetMetaData rsMeta = resultSet.getMetaData();
        Map<String, Integer> dbColumnNames = new HashMap<String, Integer>();
        for (int k = 1; k <= rsMeta.getColumnCount(); k++) {
            dbColumnNames.put(defn.formatColumnName(rsMeta.getColumnName(k)), k);
        }

        while (resultSet.next()) {
            @SuppressWarnings("unchecked")
            E tableRow = (E) DBRow.getDBRow(dummy.getClass());
//            tableRow.setDatabase(database);

            List<PropertyWrapper> fields = tableRow.getPropertyWrappers();

            for (PropertyWrapper field : fields) {
                if (field.isColumn()) {
                    String dbColumnName = field.columnName();
                    String formattedColumnName = defn.formatColumnName(dbColumnName);
                    Integer dbColumnIndex = dbColumnNames.get(formattedColumnName);
                    if (formattedColumnName != null && dbColumnIndex != null) {
                        setObjectFieldValueToColumnValue(rsMeta, dbColumnIndex, field, tableRow, resultSet, dbColumnName);
                    }
                }
            }
            tableRow.setDefined(true);
            dbTable.listOfRows.add(tableRow);
        }
    }

    private void setObjectFieldValueToColumnValue(ResultSetMetaData rsMeta, int dbColumnIndex, PropertyWrapper field, DBRow tableRow, ResultSet resultSet, String dbColumnName) throws SQLException {
        QueryableDatatype qdt = tableRow.getQueryableValueOfPropertWrapper(field);
        int columnType = rsMeta.getColumnType(dbColumnIndex);
//        int precision = rsMeta.getPrecision(dbColumnIndex);
        switch (columnType) {
            case Types.BIT:
            case Types.INTEGER:
            case Types.BIGINT:
            case Types.BOOLEAN:
            case Types.ROWID:
            case Types.SMALLINT:
            case Types.DECIMAL:
            case Types.DOUBLE:
            case Types.FLOAT:
            case Types.NUMERIC:
            case Types.REAL:
            case Types.CHAR:
            case Types.NCHAR:
            case Types.NVARCHAR:
            case Types.VARCHAR:
            case Types.LONGNVARCHAR:
            case Types.LONGVARCHAR:
            case Types.CLOB:
            case Types.NCLOB:
            case Types.DATE:
            case Types.TIME:
            case Types.TIMESTAMP:
            case Types.BINARY:
            case Types.VARBINARY:
            case Types.JAVA_OBJECT:
            case Types.LONGVARBINARY:
            case Types.BLOB:
            case Types.OTHER:
                qdt.setFromResultSet(resultSet, dbColumnName);
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
            throw new UndefinedPrimaryKeyException(thisClass);
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

    private DBTable<E> getRows(String selectStatement) throws SQLException {
        this.listOfRows.clear();
        if (printSQLBeforeExecuting || database.isPrintSQLBeforeExecuting()) {
            System.out.println(selectStatement);
        }

        Statement statement = database.getDBStatement();
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

        DBDefinition defn = database.getDefinition();
        String whereClause = defn.beginAndLine() + defn.formatColumnName(getPrimaryKeyColumn()) + defn.getEqualsComparator() + " '" + escapeSingleQuotes(pkValue.toString()) + "'";
        String selectStatement = this.getSQLForSelect() + whereClause + getOrderByClause() + database.getDefinition().endSQLStatement();
        this.getRows(selectStatement);
        return this;
    }

    public DBTable<E> getRowsByPrimaryKey(Number pkValue) throws SQLException {
        DBDefinition defn = database.getDefinition();
        String whereClause = defn.beginAndLine() + defn.formatColumnName(getPrimaryKeyColumn()) + defn.getEqualsComparator() + pkValue + " ";
        String selectStatement = this.getSQLForSelect() + whereClause + getOrderByClause() + database.getDefinition().endSQLStatement();
        this.getRows(selectStatement);
        return this;
    }

    public DBTable<E> getRowsByPrimaryKey(Date pkValue) throws SQLException {
        DBDefinition defn = database.getDefinition();
        String whereClause = defn.beginAndLine() + defn.formatColumnName(getPrimaryKeyColumn()) + defn.getEqualsComparator() + defn.getDateFormattedForQuery(pkValue) + " ";
        String selectStatement = this.getSQLForSelect() + whereClause + getOrderByClause() + database.getDefinition().endSQLStatement();
        this.getRows(selectStatement);
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
        dummy = queryTemplate;
        String whereClause = getSQLForExample(queryTemplate);
        String selectStatement = this.getSQLForSelect() + whereClause + getOrderByClause() + database.getDefinition().endSQLStatement();

        return getRows(selectStatement);
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
        return query.getWhereClause(database);
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
            String whereClause = sqlWhereClause.replaceAll("\\s*;\\s*$", "");
            String selectStatement = this.getSQLForSelect() + whereClause + getOrderByClause() + database.getDefinition().endSQLStatement();
            return getRows(selectStatement);
        } else {
            String whereClause = " AND " + sqlWhereClause.replaceAll("\\s*;\\s*$", "");
            String selectStatement = this.getSQLForSelect() + whereClause + getOrderByClause() + database.getDefinition().endSQLStatement();
            return getRows(selectStatement);
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
     * Returns the first row of the table
     *
     * particularly helpful when you know there is only one row
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

    /**
     *
     * Returns the first row and only row of the table.
     *
     * Similar to getFirstRow() but throws an UnexpectedNumberOfRowsException if
     * there is more than 1 row available
     *
     * @return
     */
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
        Statement statement = database.getDBStatement();
        DBActionList allInserts = getSQLForInsert(newRows);
        for (DBAction action : allInserts) {
            if (printSQLBeforeExecuting || database.isPrintSQLBeforeExecuting()) {
                System.out.println(action.getSQLRepresentation());
            }
            if (action.canBeBatched() && database.getBatchSQLStatementsWhenPossible()) {
                statement.addBatch(action.getSQLRepresentation());
            } else {
                statement.executeBatch();
                statement.clearBatch();
                action.execute(database, statement);
            }
        }
        statement.executeBatch();
        // Hasn't thrown an exception so they are now defined.
        for (DBAction action : allInserts) {
            action.getRow().setDefined(true);
        }
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
        return getSQLForInsert(arrayList).get(0).getSQLRepresentation();
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
    public DBActionList getSQLForInsert(List<E> newRows) {
        DBDefinition defn = database.getDefinition();
        DBActionList allInserts = new DBActionList();
        for (E row : newRows) {
            String sql =
                    defn.beginInsertLine()
                    + defn.formatTableName(row.getTableName())
                    + defn.beginInsertColumnList()
                    + this.getAllFieldsForInsert()
                    + defn.endInsertColumnList()
                    + row.getValuesClause(database)
                    + defn.endInsertLine();
            allInserts.add(new DBSave(row, sql));
            if (row.hasLargeObjectColumns()) {
                allInserts.addAll(row.getLargeObjectActions(database));
            }
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
        Statement statement = database.getDBStatement();
        List<String> allSQL = getSQLForDelete(oldRows);
        if (database.getBatchSQLStatementsWhenPossible()) {
            for (String sql : allSQL) {
                if (printSQLBeforeExecuting || database.isPrintSQLBeforeExecuting()) {
                    System.out.println(sql);
                }
                statement.addBatch(sql);
            }
            statement.executeBatch();
        } else {
            for (String sql : allSQL) {
                database.printSQLIfRequested(sql);
                statement.execute(sql);
            }
        }
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
        DBDefinition defn = database.getDefinition();
        List<String> allDeletes = new ArrayList<String>();
        for (E row : oldRows) {
//            row.setDatabase(database);
            if (row.getDefined()) {
                final QueryableDatatype primaryKey = row.getPrimaryKey();
                if (primaryKey == null) {
                    allDeletes.add(getSQLForDeleteWithoutPrimaryKey(row));
                } else {
                    String sql =
                            defn.beginDeleteLine()
                            + defn.formatTableName(row.getTableName())
                            + defn.beginWhereClause()
                            + defn.formatColumnName(this.getPrimaryKeyColumn())
                            + defn.getEqualsComparator()
                            + primaryKey.getSQLValue(database)
                            + defn.endDeleteLine();
                    allDeletes.add(sql);
                }
            } else {
                // Delete by example
                String sql =
                        defn.beginDeleteLine()
                        + defn.formatTableName(row.getTableName())
                        + defn.beginWhereClause()
                        + defn.getTrueOperation()
                        + getSQLForExample(row)
                        + defn.endDeleteLine();
                allDeletes.add(sql);
            }
        }
        return allDeletes;
    }

    /**
     *
     * @param row
     * @return
     */
    public String getSQLForDeleteWithoutPrimaryKey(E row) {
        DBDefinition defn = database.getDefinition();
        String sql =
                defn.beginDeleteLine()
                + defn.formatTableName(row.getTableName())
                + defn.beginWhereClause()
                + defn.getTrueOperation();
        for (QueryableDatatype qdt : row.getQueryableDatatypes()) {
            sql = sql
                    + defn.beginAndLine()
                    + row.getDBColumnName(qdt)
                    + defn.getEqualsComparator()
                    + (qdt.hasChanged()?qdt.getPreviousSQLValue(database):qdt.getSQLValue(database));
        }
        sql = sql + defn.endDeleteLine();
        return sql;
    }

    /**
     *
     * @param oldRows
     * @return
     */
    public List<String> getSQLForDeleteWithoutPrimaryKey(List<E> oldRows) {
        List<String> allInserts = new ArrayList<String>();
        for (E row : oldRows) {
            allInserts.add(getSQLForDeleteWithoutPrimaryKey(row));
        }
        return allInserts;
    }

    public String getSQLForUpdateWithoutPrimaryKey(E row) {
        DBDefinition defn = database.getDefinition();
        String sql =
                defn.beginUpdateLine()
                + defn.formatTableName(row.getTableName())
                + defn.beginSetClause()
                + row.getSetClause(database)
                + defn.beginWhereClause()
                + defn.getTrueOperation();
        for (QueryableDatatype qdt : row.getQueryableDatatypes()) {
            if (qdt.isNull()) {
                DBIsNullOperator isNullOp = new DBIsNullOperator();
                sql = sql
                        + isNullOp.generateWhereLine(database, row.getDBColumnName(qdt));
            } else {
                sql = sql
                        + defn.beginAndLine()
                        + row.getDBColumnName(qdt)
                        + defn.getEqualsComparator()
                        + (qdt.hasChanged()?qdt.getPreviousSQLValue(database):qdt.getSQLValue(database));
            }
        }
        sql = sql + defn.endDeleteLine();
        return sql;
    }

    public List<String> getSQLForUpdateWithoutPrimaryKey(List<E> rows) {
        List<String> updates = new ArrayList<String>();
        for (E row : rows) {
            updates.add(getSQLForUpdateWithoutPrimaryKey(row));
        }
        return updates;
    }

    public void update(E oldRow) throws SQLException {
        ArrayList<E> arrayList = new ArrayList<E>();
        arrayList.add(oldRow);
        update(arrayList);
    }

    public void update(List<E> oldRows) throws SQLException {
        Statement statement = database.getDBStatement();
        List<String> allSQL = getSQLForUpdate(oldRows);
        if (database.getBatchSQLStatementsWhenPossible()) {
            for (String sql : allSQL) {
                if (printSQLBeforeExecuting || database.isPrintSQLBeforeExecuting()) {
                    System.out.println(sql);
                }
                statement.addBatch(sql);
            }
            statement.executeBatch();
        } else {
            for (String sql : allSQL) {
                statement.execute(sql);
            }
        }
        for (DBRow row : oldRows) {
//            row.setDatabase(database);
            row.setUnchanged();
//            row.getPrimaryKey().setUnchanged();
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
        DBDefinition defn = database.getDefinition();
        List<String> allSQL = new ArrayList<String>();
        for (E row : oldRows) {
            QueryableDatatype primaryKey = row.getPrimaryKey();
            if (primaryKey == null) {
                allSQL.add(getSQLForUpdateWithoutPrimaryKey(row));
            } else {
                String pkOriginalValue = (primaryKey.hasChanged() ? primaryKey.getPreviousSQLValue(database) : primaryKey.getSQLValue(database));
                String sql =
                        defn.beginUpdateLine()
                        + defn.formatTableName(row.getTableName())
                        + defn.beginSetClause()
                        + row.getSetClause(database)
                        + defn.beginWhereClause()
                        + defn.formatColumnName(this.getPrimaryKeyColumn())
                        + defn.getEqualsComparator()
                        + pkOriginalValue
                        + defn.endDeleteLine();
                allSQL.add(sql);
            }
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

    public List<Long> getPrimaryKeysAsLong() {
        List<Long> primaryKeys = new ArrayList<Long>();
        for (E e : listOfRows) {
            primaryKeys.add(e.getPrimaryKey().longValue());
        }
        return primaryKeys;
    }

    public List<String> getPrimaryKeysAsString() {
        List<String> primaryKeys = new ArrayList<String>();
        for (E e : listOfRows) {
            primaryKeys.add(e.getPrimaryKey().toString());
        }
        return primaryKeys;
    }

    /**
     * Compares 2 tables, presumably from different criteria or databases prints
     * the differences to System.out
     *
     * Should be updated to return the varying rows somehow
     *
     * @param secondTable : a comparable table
     */
    public void compare(DBTable<E> secondTable) {
        HashMap<Long, E> secondMap = new HashMap<Long, E>();
        for (E row : secondTable.toList()) {
            secondMap.put(row.getPrimaryKey().longValue(), row);
        }
        for (E row : this.toList()) {
            E foundRow = secondMap.get(row.getPrimaryKey().longValue());
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
        DBDefinition defn = database.getDefinition();
        if (sortOrder != null) {
            StringBuilder orderByClause = new StringBuilder(defn.beginOrderByClause());
            String sortSeparator = defn.getStartingOrderByClauseSeparator();
            for (QueryableDatatype qdt : sortOrder) {
                final String dbColumnName = sortBase.getDBColumnName(qdt);
                if (dbColumnName != null) {
                    orderByClause.append(sortSeparator).append(defn.formatColumnName(dbColumnName)).append(defn.getOrderByDirectionClause(qdt.getSortOrder()));
                    sortSeparator = defn.getSubsequentOrderByClauseSeparator();
                }
            }
            orderByClause.append(defn.endOrderByClause());
            return orderByClause.toString();
        }
        return "";
    }
}
