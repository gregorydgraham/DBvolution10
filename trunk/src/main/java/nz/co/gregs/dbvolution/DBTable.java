package nz.co.gregs.dbvolution;

import nz.co.gregs.dbvolution.actions.DBActionList;
import nz.co.gregs.dbvolution.actions.DBInsert;
import java.io.PrintStream;
import java.sql.Date;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Types;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import nz.co.gregs.dbvolution.actions.DBDelete;
import nz.co.gregs.dbvolution.actions.DBUpdate;

import nz.co.gregs.dbvolution.annotations.DBSelectQuery;
import nz.co.gregs.dbvolution.databases.DBStatement;
import nz.co.gregs.dbvolution.databases.definitions.DBDefinition;
import nz.co.gregs.dbvolution.datatypes.QueryableDatatype;
import nz.co.gregs.dbvolution.exceptions.AccidentalBlankQueryException;
import nz.co.gregs.dbvolution.exceptions.IncorrectDBRowInstanceSuppliedException;
import nz.co.gregs.dbvolution.exceptions.UndefinedPrimaryKeyException;
import nz.co.gregs.dbvolution.exceptions.UnexpectedNumberOfRowsException;
import nz.co.gregs.dbvolution.internal.PropertyWrapper;

/**
 *
 * @param <E>
 * @author gregory.graham
 */
public class DBTable<E extends DBRow> {

    private static final long serialVersionUID = 1L;
//    private static boolean printSQLBeforeExecuting = false;
    E template;
    private DBDatabase database = null;
    ResultSet resultSet = null;
    private java.util.ArrayList<E> listOfRows = new java.util.ArrayList<E>();
    private Long rowLimit;
    private List<PropertyWrapper> sortOrder = null;
    private boolean blankQueryAllowed = false;

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
        template = dummyObject;
    }

    private String getAllFieldsForSelect() {
        StringBuilder allFields = new StringBuilder();
        List<String> columnNames = template.getColumnNames();
        String separator = "";
        for (String column : columnNames) {
            allFields.append(separator).append(" ").append(database.getDefinition().formatColumnName(column));
            separator = ",";
        }

        return allFields.toString();
    }

    private String getSQLForSelectAll() {
        DBDefinition defn = database.getDefinition();
        StringBuilder selectStatement = new StringBuilder();
        DBSelectQuery selectQueryAnnotation = template.getClass().getAnnotation(DBSelectQuery.class);
        if (selectQueryAnnotation != null) {
            selectStatement.append(selectQueryAnnotation.value());
        } else {
            selectStatement.append(defn.beginSelectStatement());
            if (rowLimit != null) {
                selectStatement.append(defn.getLimitRowsSubClauseDuringSelectClause(rowLimit));
            }
//            String tableAlias = ("_"+dummy.getClass().getSimpleName().hashCode()).replaceAll("-", "_");
            selectStatement.append(getAllFieldsForSelect())
                    .append(defn.beginFromClause())
                    .append(defn.formatTableName(template))
                    .append(defn.beginTableAlias()).append(defn.getTableAlias(template)).append(defn.endTableAlias())
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
        DBSelectQuery selectQueryAnnotation = template.getClass().getAnnotation(DBSelectQuery.class);
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

//            String tableAlias = ("_"+dummy.getClass().getSimpleName().hashCode()).replaceAll("-", "_");
            selectStatement
                    .append(getAllFieldsForSelect())
                    .append(defn.beginFromClause())
                    .append(defn.formatTableName(template))
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
     * @return
     * @throws SQLException, AccidentalBlankQueryException
     */
    public DBTable<E> getAllRows() throws SQLException, AccidentalBlankQueryException {
        if (!this.blankQueryAllowed){
            throw new AccidentalBlankQueryException();
        }
        resultSet = null;
        this.listOfRows.clear();

        String selectStatement = this.getSQLForSelectAll();

        resultSet = null;
        DBStatement statement = this.database.getDBStatement();
        try {
            try {
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
            } finally {
                if (resultSet != null) {
                    resultSet.close();
                }
            }
        } finally {
            statement.close();
        }
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
            E tableRow = (E) DBRow.getDBRow(template.getClass());

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
        QueryableDatatype qdt = field.getQueryableDatatype();
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
                
                // ensure field set when using type adaptors
            	field.setQueryableDatatype(qdt);
                break;
            default:
                throw new RuntimeException("Unknown Java SQL Type: " + rsMeta.getColumnType(dbColumnIndex));
        }
    }

    private String getPrimaryKeyColumnName() {
        String columnName = template.getPrimaryKeyColumnName();
        if (columnName == null) {
            throw new UndefinedPrimaryKeyException(template.getClass());
        } else {
            return columnName;
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
//        if (printSQLBeforeExecuting || database.isPrintSQLBeforeExecuting()) {
//            System.out.println(selectStatement);
//        }

        DBStatement statement = database.getDBStatement();
        try {
            boolean executed = statement.execute(selectStatement);
            resultSet = statement.getResultSet();
            try {
                addAllFields(this, resultSet);
            } finally {
                resultSet.close();
            }
        } finally {
            statement.close();
        }
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
        String whereClause = defn.beginAndLine() + defn.formatColumnName(getPrimaryKeyColumnName()) + defn.getEqualsComparator() + " '" + escapeSingleQuotes(pkValue.toString()) + "'";
        String selectStatement = this.getSQLForSelect() + whereClause + getOrderByClause() + database.getDefinition().endSQLStatement();
        this.getRows(selectStatement);
        return this;
    }

    public DBTable<E> getRowsByPrimaryKey(Number pkValue) throws SQLException {
        DBDefinition defn = database.getDefinition();
        String whereClause = defn.beginAndLine() + defn.formatColumnName(getPrimaryKeyColumnName()) + defn.getEqualsComparator() + pkValue + " ";
        String selectStatement = this.getSQLForSelect() + whereClause + getOrderByClause() + database.getDefinition().endSQLStatement();
        this.getRows(selectStatement);
        return this;
    }

    public DBTable<E> getRowsByPrimaryKey(Date pkValue) throws SQLException {
        DBDefinition defn = database.getDefinition();
        String whereClause = defn.beginAndLine() + defn.formatColumnName(getPrimaryKeyColumnName()) + defn.getEqualsComparator() + defn.getDateFormattedForQuery(pkValue) + " ";
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
    public DBTable<E> getRowsByExample(E queryTemplate) throws SQLException, AccidentalBlankQueryException {
        template = queryTemplate;
        String whereClause = getSQLForExample(queryTemplate);
        String selectStatement = this.getSQLForSelect() + whereClause + getOrderByClause() + database.getDefinition().endSQLStatement();

        return getRows(selectStatement);
    }

    public E getOnlyRowByExample(E queryTemplate) throws SQLException, UnexpectedNumberOfRowsException, AccidentalBlankQueryException {
        return getRowsByExample(queryTemplate, 1).listOfRows.get(0);
    }

    public DBTable<E> getRowsByExample(E queryTemplate, int expectedNumberOfRows) throws SQLException, UnexpectedNumberOfRowsException, AccidentalBlankQueryException {
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
     * @param row
     * @return
     */
    public String getSQLForExample(E row) throws AccidentalBlankQueryException {
        if (!this.blankQueryAllowed && row.willCreateBlankQuery(database)) {
            throw new AccidentalBlankQueryException();
        }
        return row.getWhereClause(database);
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
     * @throws java.sql.SQLException
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
     * @throws java.sql.SQLException
     */
    public void print() throws SQLException, AccidentalBlankQueryException {
        if (resultSet==null){
            getRowsByExample(template);
        }
        print(System.out);
    }

    /**
     * the same as print() but allows you to specify the PrintStream required
     *
     * myTable.printAllRows(System.err);
     *
     * @param ps
     * @throws java.sql.SQLException
     */
    public void print(PrintStream ps) throws SQLException, AccidentalBlankQueryException {
        if (resultSet==null){
            getRowsByExample(template);
        }
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
     * @throws java.sql.SQLException
     */
    public E getFirstRow() throws SQLException, AccidentalBlankQueryException {
        if (resultSet==null){
            getRowsByExample(template);
        }
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
     * @throws nz.co.gregs.dbvolution.exceptions.UnexpectedNumberOfRowsException
     * @throws java.sql.SQLException
     */
    public E getOnlyRow() throws UnexpectedNumberOfRowsException, SQLException {
        if (resultSet==null){
            getRowsByExample(template);
        }
        if (this.listOfRows.size() > 0) {
            return this.listOfRows.get(0);
        } else {
            throw new UnexpectedNumberOfRowsException(1, listOfRows.size(), "Unexpected Number Of Rows Detected: was expecting 1, found " + listOfRows.size());
        }
    }

//    @SafeVarargs
    public final DBActionList insert(E... newRows) throws SQLException {
        DBActionList actions = new DBActionList();
        for (E row : newRows) {
            actions.addAll(DBInsert.save(database, row));
        }
        return actions;
    }

    public DBActionList insert(List<E> newRows) throws SQLException {
        DBActionList changes = new DBActionList();
        for (DBRow row : newRows) {
            changes.addAll(DBInsert.save(database, row));
        }
        return changes;
    }

//    @SafeVarargs
    public final DBActionList delete(E... oldRows) throws SQLException {
        DBActionList actions = new DBActionList();
        for (E row : oldRows) {
            actions.addAll(DBDelete.delete(database, row));
        }
        return actions;
    }

    /**
     *
     * @param oldRows
     * @return 
     * @throws SQLException
     */
    public DBActionList delete(List<E> oldRows) throws SQLException {
        DBActionList actions = new DBActionList();
        for (E row : oldRows) {
            actions.addAll(DBDelete.delete(database, row));
        }
        return actions;
    }

    public DBActionList update(E oldRow) throws SQLException {
        return DBUpdate.update(database, oldRow);
    }

    public DBActionList update(List<E> oldRows) throws SQLException {
        DBActionList changes = new DBActionList();
        for (E row : oldRows) {
            if (row.hasChangedSimpleTypes()) {
                changes.addAll(DBUpdate.update(database, row));
            }
        }
        return changes;
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
     * @throws java.sql.SQLException
     */
    public List<E> toList() throws SQLException, AccidentalBlankQueryException {
        if (resultSet==null){
            getRowsByExample(template);
        }
        return new java.util.ArrayList<E>(listOfRows);
    }

    public List<Long> getPrimaryKeysAsLong() throws SQLException {
        if (resultSet==null){
            getRowsByExample(template);
        }
        List<Long> primaryKeys = new ArrayList<Long>();
        for (E e : listOfRows) {
            primaryKeys.add(e.getPrimaryKey().longValue());
        }
        return primaryKeys;
    }

    public List<String> getPrimaryKeysAsString() throws SQLException {
        if (resultSet==null){
            getRowsByExample(template);
        }
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
     * @throws java.sql.SQLException
     */
    public void compare(DBTable<E> secondTable) throws SQLException {
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

    public DBTable<E> setRowLimit(int i) {
        resultSet = null;
        rowLimit = new Long(i);
        return this;
    }

    public DBTable<E> clearRowLimit() {
        resultSet = null;
        rowLimit = null;
        return this;
    }

    /**
     * Sets the sort order of properties (field and/or method) by the given
     * property object references.
     *
     * <p>
     * For example the following code snippet will sort by just the name column:
     * <pre>
     * Customer customer = ...;
     * customer.setSortOrder(customer, customer.name);
     * </pre>
     *
     * <p>
     * Requires that all {@literal orderColumns} be from the {@code baseRow}
     * instance to work.
     *
     * @param baseRow
     *
     * @param orderColumns
     * @return this
     */
    public DBTable<E> setSortOrder(E baseRow, QueryableDatatype... orderColumns) {
        resultSet = null;
        sortOrder = new ArrayList<PropertyWrapper>();
        for (QueryableDatatype qdt : orderColumns) {
            PropertyWrapper prop = baseRow.getPropertyWrapperOf(qdt);
            if (prop == null) {
                throw new IncorrectDBRowInstanceSuppliedException(baseRow, qdt);
            }
            sortOrder.add(prop);
        }
        return this;
    }

    public DBTable<E> clearSortOrder() {
        resultSet = null;
        sortOrder = null;
        return this;
    }

    private String getOrderByClause() {
        DBDefinition defn = database.getDefinition();
        if (sortOrder != null) {
            StringBuilder orderByClause = new StringBuilder(defn.beginOrderByClause());
            String sortSeparator = defn.getStartingOrderByClauseSeparator();
            for (PropertyWrapper prop : sortOrder) {
                QueryableDatatype qdt = prop.getQueryableDatatype();
                final String dbColumnName = prop.columnName();
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

    public DBTable<E> setBlankQueryAllowed(boolean allow) {
        this.blankQueryAllowed = allow;
        return this;
    }
}
