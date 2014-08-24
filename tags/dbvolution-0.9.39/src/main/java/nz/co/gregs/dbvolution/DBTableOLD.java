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
import nz.co.gregs.dbvolution.datatypes.DBNumber;
import nz.co.gregs.dbvolution.datatypes.QueryableDatatype;
import nz.co.gregs.dbvolution.exceptions.AccidentalBlankQueryException;
import nz.co.gregs.dbvolution.exceptions.IncorrectRowProviderInstanceSuppliedException;
import nz.co.gregs.dbvolution.exceptions.UndefinedPrimaryKeyException;
import nz.co.gregs.dbvolution.exceptions.UnexpectedNumberOfRowsException;
import nz.co.gregs.dbvolution.exceptions.UnknownJavaSQLTypeException;
import nz.co.gregs.dbvolution.expressions.DBExpression;
import nz.co.gregs.dbvolution.internal.properties.PropertyWrapper;
import nz.co.gregs.dbvolution.query.QueryOptions;

/**
 * <p>
 * DBvolution is available on <a
 * href="https://sourceforge.net/projects/dbvolution/">SourceForge</a> complete
 * with <a href="https://sourceforge.net/p/dbvolution/blog/">BLOG</a>
 *
 * <p>
 * DBTableOLD provides features for making simple queries on the database.
 *
 * <p>
 * If your query only references one table, DBTableOLD makes it easy to get the
 * rows from that table.
 *
 * <p>
 * Use
 * {@link DBDatabase#getDBTable(nz.co.gregs.dbvolution.DBRow) getDBTable from DBDatabase}
 * to retrieve an instance for particular DBRow subclass.
 *
 * <p>
 * DBTableOLD and {@link DBQuery} are very similar but there are important
 * differences. In particular DBTableOLD uses a simple
 * {@code List<<E extends DBRow>>} rather than {@code List<DBQueryRow>} and
 * DBTableOLD requires you to specify an example in
 * {@link #getRowsByExample(nz.co.gregs.dbvolution.DBRow)} rather than using the
 * exemplar provided initially.
 *
 * <p>
 * DBTableOLD is a quick and easy API for targeted data retrieval, for more complex
 * needs use {@link DBQuery}.
 *
 * @param <E>
 * @author Gregory Graham
 */
@Deprecated
public class DBTableOLD<E extends DBRow> {

//    private static boolean printSQLBeforeExecuting = false;
    E template;
    private DBDatabase database = null;
    ResultSet resultSet = null;
    private final java.util.ArrayList<E> listOfRows = new java.util.ArrayList<E>();
    private Long rowLimit;
    private List<PropertyWrapper> sortOrder = null;
    private boolean blankQueryAllowed = false;
    private final QueryOptions options = new QueryOptions();

    /**
     * Factory method to create a DBTableOLD.
     *
     * <p>
     * {@link DBDatabase#getDBTable(nz.co.gregs.dbvolution.DBRow) } is probably
     * a better option.
     *
     * @param <E>
     * @param database
     * @param example
     * @return an instance of the supplied example
     */
    public static <E extends DBRow> DBTableOLD<E> getInstance(DBDatabase database, E example) {
        DBTableOLD<E> dbTable = new DBTableOLD<E>(database, example);
        return dbTable;
    }

    /**
     * Constructor
     *
     * @param myDatabase
     * @param dummyObject
     */
    private DBTableOLD(DBDatabase myDatabase, E dummyObject) {
        this.database = myDatabase;
        template = dummyObject;
    }

    private String getAllFieldsForSelect() {
        StringBuilder allFields = new StringBuilder();
        List<String> columnNames = template.getColumnNames(database);
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
                selectStatement.append(defn.getLimitRowsSubClauseDuringSelectClause(options));
            }
//            String tableAlias = ("_"+dummy.getClass().getSimpleName().hashCode()).replaceAll("-", "_");
            selectStatement.append(getAllFieldsForSelect())
                    .append(defn.beginFromClause())
                    .append(defn.formatTableName(template))
                    .append(defn.beginTableAlias()).append(defn.getTableAlias(template)).append(defn.endTableAlias())
                    .append(getOrderByClause())
                    .append(defn.getLimitRowsSubClauseAfterWhereClause(options))
                    .append(defn.endSQLStatement());
        }

        return selectStatement.toString();
    }

    /**
     * Renamed {@link #getSQLSelectAndFromForQuery() }
     *
     * @return the SQL string for the SELECT and FROM clauses
     * @deprecated
     * @see #getSQLSelectAndFromForQuery() 
     */
    @Deprecated
    public String getSQLForSelect() {
        return getSQLSelectAndFromForQuery();
    }
    /**
     * Returns the SELECT and FROM clauses used in the SQL query.
     *
     * @return the SQL string for the SELECT and FROM clauses
     */
    public String getSQLSelectAndFromForQuery() {
        DBDefinition defn = database.getDefinition();
        StringBuilder selectStatement = new StringBuilder();
        DBSelectQuery selectQueryAnnotation = template.getClass().getAnnotation(DBSelectQuery.class);
        if (selectQueryAnnotation != null) {
            selectStatement
                    .append(selectQueryAnnotation.value())
                    .append(defn.beginWhereClause())
                    .append(defn.getWhereClauseBeginningCondition(options));
        } else {
            selectStatement.append(defn.beginSelectStatement());
            if (rowLimit != null) {
                selectStatement.append(defn.getLimitRowsSubClauseDuringSelectClause(options));
            }

//            String tableAlias = ("_"+dummy.getClass().getSimpleName().hashCode()).replaceAll("-", "_");
            selectStatement
                    .append(getAllFieldsForSelect())
                    .append(defn.beginFromClause())
                    .append(defn.formatTableName(template))
                    .append(defn.beginWhereClause())
                    .append(defn.getWhereClauseBeginningCondition(options));
        }

        return selectStatement.toString();
    }

    /**
     * Gets All Rows of the table from the database
     *
     * <p>
     * Use this carefully as it does what it says on the label: Gets All Rows of
     * the table from the database.
     *
     * <p>
     * throws AccidentalBlankQueryException if you haven't specifically allowed
     * blank queries with setBlankQueryAllowed(boolean)
     *
     * @return ALL rows of the table from the database
     * @throws SQLException, AccidentalBlankQueryException
     */
    public DBTableOLD<E> getAllRows() throws SQLException, AccidentalBlankQueryException {
        if (!this.blankQueryAllowed) {
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

    private void addAllFields(DBTableOLD<E> dbTable, ResultSet resultSet) throws SQLException {
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
                        QueryableDatatype qdt = field.getQueryableDatatype();
                        if (tableRow.isEmptyRow() && !qdt.isNull()) {
                            tableRow.setEmptyRow(false);
                        }
                    }
                }
            }
            tableRow.setDefined();
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
                qdt.setFromResultSet(database, resultSet, dbColumnName);

                // ensure field set when using type adaptors
                field.setQueryableDatatype(qdt);
                break;
            default:
                throw new UnknownJavaSQLTypeException("Unknown Java SQL Type: table " + tableRow.getTableName() + " column " + dbColumnName + " has a Unknown SQL type of " + rsMeta.getColumnType(dbColumnIndex) + ". Please contact enquiry at https://sourceforge.net/projects/dbvolution/ for support.", rsMeta.getColumnType(dbColumnIndex));
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

    private DBTableOLD<E> getRows(String selectStatement) throws SQLException {
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
     * primary key.
     *
     * <p>
     * The primary key column is identified by the {@code @DBPrimaryKey}
     * annotation in the TableRow subclass.
     *
     * @param pkValue
     * @return a DBTableOLD instance containing the row(s) for the primary key
     * @throws SQLException
     */
    public DBTableOLD<E> getRowsByPrimaryKey(Object pkValue) throws SQLException {

        DBDefinition defn = database.getDefinition();
        String whereClause = defn.beginConditionClauseLine(options) + defn.formatColumnName(getPrimaryKeyColumnName()) + defn.getEqualsComparator() + " '" + escapeSingleQuotes(pkValue.toString()) + "'";
        String selectStatement = this.getSQLSelectAndFromForQuery() + whereClause + getOrderByClause() + database.getDefinition().endSQLStatement();
        this.getRows(selectStatement);
        return this;
    }

    /**
     * Retrieves the row (or rows in a bad database) that has the specified
     * primary key.
     *
     * <p>
     * The primary key column is identified by the {@code @DBPrimaryKey}
     * annotation in the TableRow subclass.
     *
     * @param pkValue
     * @return a DBTableOLD instance containing the row(s) for the primary key
     * @throws SQLException
     */
    public DBTableOLD<E> getRowsByPrimaryKey(Number pkValue) throws SQLException {
        DBDefinition defn = database.getDefinition();
        String whereClause = defn.beginConditionClauseLine(options) + defn.formatColumnName(getPrimaryKeyColumnName()) + defn.getEqualsComparator() + pkValue + " ";
        String selectStatement = this.getSQLSelectAndFromForQuery() + whereClause + getOrderByClause() + database.getDefinition().endSQLStatement();
        this.getRows(selectStatement);
        return this;
    }

    /**
     * Retrieves the row (or rows in a bad database) that has the specified
     * primary key.
     *
     * <p>
     * The primary key column is identified by the {@code @DBPrimaryKey}
     * annotation in the TableRow subclass.
     *
     * @param pkValue
     * @return a DBTableOLD instance containing the row(s) for the primary key
     * @throws SQLException
     */
    public DBTableOLD<E> getRowsByPrimaryKey(Date pkValue) throws SQLException {
        DBDefinition defn = database.getDefinition();
        String whereClause = defn.beginConditionClauseLine(options) + defn.formatColumnName(getPrimaryKeyColumnName()) + defn.getEqualsComparator() + defn.getDateFormattedForQuery(pkValue) + " ";
        String selectStatement = this.getSQLSelectAndFromForQuery() + whereClause + getOrderByClause() + database.getDefinition().endSQLStatement();
        this.getRows(selectStatement);
        return this;
    }

    /**
     * This method retrieves all the appropriate records.
     *
     * <p>
     * The following will retrieve all records from the table where the Language
     * column contains JAVA:<br>
     * {@code DBTableOLD<MyRow> myTable = database.getDBTableOLD(new MyRow());}<br>
     * {@code MyRow myExample = new MyRow();}<br>
     * {@code myExample.getLanguage.useLikeComparison("%JAVA%"); }<br>
     * {@code myTable.getByExample(myExample); }<br>
     * {@code List<MyRow> myRows = myTable.toList();}
     *
     * @param queryTemplate
     * @return a DBTableOLD instance containing the rows that match the example
     * @throws SQLException
     * @see QueryableDatatype
     * @see DBRow
     */
    public DBTableOLD<E> getRowsByExample(E queryTemplate) throws SQLException, AccidentalBlankQueryException {
        template = queryTemplate;
        String whereClause = getSQLWhereClauseForExample(queryTemplate);
        String selectStatement = this.getSQLSelectAndFromForQuery() + whereClause + getOrderByClause() + database.getDefinition().endSQLStatement();

        return getRows(selectStatement);
    }

    /**
     * This method retrieves the only appropriate record.
     *
     * <p>
     * Throws an exception if there is no appropriate records, or several
     * appropriate records.
     *
     * <p>
     * The following will the only record from the table where the Language
     * column contains JAVA:<br>
     * {@code MyTableRow myExample = new MyTableRow();}<br>
     * {@code myExample.getLanguage.useLikeComparison("%JAVA%"); }<br>
     * {@code (new DBTableOLD<MyTableRow>()).getOnlyRowByExample(myExample);}
     *
     * @param queryTemplate
     * @return a DBTableOLD instance containing the rows that match the example
     * @throws SQLException
     * @throws UnexpectedNumberOfRowsException
     * @throws AccidentalBlankQueryException
     * @see QueryableDatatype
     * @see DBRow
     */
    public E getOnlyRowByExample(E queryTemplate) throws SQLException, UnexpectedNumberOfRowsException, AccidentalBlankQueryException {
        return getRowsByExample(queryTemplate, 1).listOfRows.get(0);
    }

    /**
     * This method retrieves all the appropriate records, and throws an
     * exception if the number of records differs from the required number.
     *
     * <p>
     * The following will retrieve all 10 records from the table where the
     * Language column contains JAVA, and throw an exception if anything other
     * than 10 rows is returned.<br>
     * {@code MyTableRow myExample = new MyTableRow();}<br>
     * {@code myExample.getLanguage.useLikeComparison("%JAVA%"); }<br>
     * {@code (new DBTableOLD<MyTableRow>()).getRowsByExample(myExample, 10L);}
     *
     * @param queryTemplate
     * @param expectedNumberOfRows
     * @return a DBTableOLD instance containing the rows that match the example
     * @throws SQLException
     * @throws nz.co.gregs.dbvolution.exceptions.UnexpectedNumberOfRowsException
     * @throws AccidentalBlankQueryException
     * @see QueryableDatatype
     * @see DBRow
     */
    public DBTableOLD<E> getRowsByExample(E queryTemplate, long expectedNumberOfRows) throws SQLException, UnexpectedNumberOfRowsException, AccidentalBlankQueryException {
        DBTableOLD<E> rowsByExample = getRowsByExample(queryTemplate);
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
     * @return a String of the WHERE clause used for the specified example
     */
    public String getSQLWhereClauseForExample(E row) throws AccidentalBlankQueryException {
        if (!this.blankQueryAllowed && row.willCreateBlankQuery(database)) {
            throw new AccidentalBlankQueryException();
        }
        StringBuilder whereClause = new StringBuilder();
        String lineSep = System.getProperty("line.separator");
        DBDefinition defn = database.getDefinition();
        List<String> tabRowCriteria = row.getWhereClausesWithoutAliases(database);
        if (tabRowCriteria != null && !tabRowCriteria.isEmpty()) {
            for (String clause : tabRowCriteria) {
                whereClause.append(lineSep).append(defn.beginConditionClauseLine(options)).append(clause);
            }
        }
        return whereClause.toString();
    }

    /**
     * Renamed {@link #getSQLWhereClauseForExample(nz.co.gregs.dbvolution.DBRow) }
     *
     * @param row
     * @return the where clause
     * @throws AccidentalBlankQueryException
     * @deprecated
     * @see #getSQLWhereClauseForExample(nz.co.gregs.dbvolution.DBRow) 
     */
    @Deprecated
    public String getSQLForExample(E row) throws AccidentalBlankQueryException {
        return getSQLWhereClauseForExample(row);
    }

    /**
     * For the particularly hard queries, just provide the actual WHERE clause
     * you want to use.
     *
     * <p>
     * Check out {@link DBExpression expressions} before using this method.
     *
     * <p>
     * myExample.getLanguage.isLike("%JAVA%"); is similar to: getByRawSQL("and
     * language like '%JAVA%'");
     *
     * <p>
     * N.B. the starting AND is optional and avoid trailing semicolons
     *
     * @param sqlWhereClause
     * @return a DBTableOLD of the rows matching the WHERE clause specified
     * @throws java.sql.SQLException
     */
    public DBTableOLD<E> getRowsByRawSQL(String sqlWhereClause) throws SQLException {
        if (sqlWhereClause.toLowerCase().matches("^\\s*and\\s+.*")) {
            String whereClause = sqlWhereClause.replaceAll("\\s*;\\s*$", "");
            String selectStatement = this.getSQLSelectAndFromForQuery() + whereClause + getOrderByClause() + database.getDefinition().endSQLStatement();
            return getRows(selectStatement);
        } else {
            String whereClause = " AND " + sqlWhereClause.replaceAll("\\s*;\\s*$", "");
            String selectStatement = this.getSQLSelectAndFromForQuery() + whereClause + getOrderByClause() + database.getDefinition().endSQLStatement();
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
        if (resultSet == null) {
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
        if (resultSet == null) {
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
     * <p>
     * particularly helpful when you know there is only one row
     *
     * <p>
     * If the no query has been run on the DBTableOLD yet, {@link #getRowsByExample(nz.co.gregs.dbvolution.DBRow)
     * } with the initial exemplar will be run.
     *
     * @return the first row in this DBTableOLD instance
     * @throws java.sql.SQLException
     */
    public E getFirstRow() throws SQLException, AccidentalBlankQueryException {
        if (resultSet == null) {
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
     * <p>
     * Similar to {@link #getFirstRow()} but throws an
     * UnexpectedNumberOfRowsException if there is more than 1 row available
     *
     * <p>
     * If the no query has been run on the DBTableOLD yet, {@link #getRowsByExample(nz.co.gregs.dbvolution.DBRow)
     * } with the initial exemplar will be run.
     *
     * @return the first row in this DBTableOLD instance
     * @throws nz.co.gregs.dbvolution.exceptions.UnexpectedNumberOfRowsException
     * @throws java.sql.SQLException
     */
    public E getOnlyRow() throws UnexpectedNumberOfRowsException, SQLException {
        if (resultSet == null) {
            getRowsByExample(template);
        }
        if (this.listOfRows.size() > 0) {
            return this.listOfRows.get(0);
        } else {
            throw new UnexpectedNumberOfRowsException(1, listOfRows.size(), "Unexpected Number Of Rows Detected: was expecting 1, found " + listOfRows.size());
        }
    }

	/**
	 *
	 * @param newRows
	 * @return a DBActionList of all the changes required.
	 * @throws SQLException
	 */
	    public final DBActionList insert(E... newRows) throws SQLException {
        DBActionList actions = new DBActionList();
        for (E row : newRows) {
            actions.addAll(DBInsert.save(database, row));
        }
        return actions;
    }

	/**
	 *
	 * @param newRows
	 * @return a DBActionList of all the changes required.
	 * @throws SQLException
	 */
	public DBActionList insert(List<E> newRows) throws SQLException {
        DBActionList changes = new DBActionList();
        for (DBRow row : newRows) {
            changes.addAll(DBInsert.save(database, row));
        }
        return changes;
    }
	
	/**
	 *
	 * @param oldRows
	 * @return a DBActionList of all the changes required.
	 * @throws SQLException
	 */
	public final DBActionList delete(E... oldRows) throws SQLException {
        DBActionList actions = new DBActionList();
        for (E row : oldRows) {
            actions.addAll(DBDelete.delete(database, row));
        }
        return actions;
    }

    /**
     * Deletes the rows from the database permanently.
     *
     * @param oldRows
     * @return a {@link DBActionList} of the delete actions.
     * @throws SQLException
     */
    public DBActionList delete(List<E> oldRows) throws SQLException {
        DBActionList actions = new DBActionList();
        for (E row : oldRows) {
            actions.addAll(DBDelete.delete(database, row));
        }
        return actions;
    }

	/**
	 *
	 * @param oldRow
	 * @return a DBActionList of all the changes required.
	 * @throws SQLException
	 */
	public DBActionList update(E oldRow) throws SQLException {
        return DBUpdate.update(database, oldRow);
    }

	/**
	 *
	 * @param oldRows
	 * @return a DBActionList of all the changes required
	 * @throws SQLException
	 */
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
     *
     * @param query
     * @param sqlWhereClause
     * @return a String of the WHERE clause for the specified example and
     * specified SQL clause
     * @see #getRowsByRawSQL(java.lang.String)
     */
    public String getSQLWhereClauseWithExampleAndRawSQL(E query, String sqlWhereClause) {
        if (sqlWhereClause.toLowerCase().matches("^\\s*and\\s+.*")) {
            return getSQLWhereClauseForExample(query) + sqlWhereClause.replaceAll("\\s*;\\s*$", "");
        } else {
            return getSQLWhereClauseForExample(query) + " AND " + sqlWhereClause.replaceAll("\\s*;\\s*$", "");
        }
    }

    /**
     * Extracts the rows from this DBTableOLD instance into a standard List
     *
     * @return a List of the rows in this DBTableOLD instance
     * @throws java.sql.SQLException
     */
    public List<E> toList() throws SQLException, AccidentalBlankQueryException {
        if (resultSet == null) {
            getRowsByExample(template);
        }
        return new java.util.ArrayList<E>(listOfRows);
    }

	/**
	 *
	 * @return a list of all the primary keys.
	 * @throws SQLException
	 */
	public List<Long> getPrimaryKeysAsLong() throws SQLException {
        if (resultSet == null) {
            getRowsByExample(template);
        }
        List<Long> primaryKeys = new ArrayList<Long>();
        for (E e : listOfRows) {
            final QueryableDatatype primaryKeyQDT = e.getPrimaryKey();
            if (primaryKeyQDT instanceof DBNumber) {
                final DBNumber pkAsDBNumber = (DBNumber) primaryKeyQDT;
                primaryKeys.add(pkAsDBNumber.longValue());
            }
        }
        return primaryKeys;
    }

	/**
	 *
	 * @return a list of all the primary keys
	 * @throws SQLException
	 */
	public List<String> getPrimaryKeysAsString() throws SQLException {
        if (resultSet == null) {
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
    public void compare(DBTableOLD<E> secondTable) throws SQLException {
        HashMap<String, E> secondMap = new HashMap<String, E>();
        for (E row : secondTable.toList()) {
            secondMap.put(row.getPrimaryKey().toString(), row);
        }
        for (E row : this.toList()) {
            E foundRow = secondMap.get(row.getPrimaryKey().toString());
            if (foundRow == null) {
                System.out.println("NOT FOUND: " + row);
            } else if (!row.toString().equals(foundRow.toString())) {
                System.out.println("DIFFERENT: " + row);
                System.out.println("         : " + foundRow);
            }
        }
    }

	/**
	 *
	 * @param i
	 * @return this DBTableOLD instance.
	 */
	public DBTableOLD<E> setRowLimit(int i) {
        resultSet = null;
        rowLimit = new Long(i);
        return this;
    }

	/**
	 *
	 * @return this DBTableOLD instance.
	 */
	public DBTableOLD<E> clearRowLimit() {
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
    public DBTableOLD<E> setSortOrder(E baseRow, QueryableDatatype... orderColumns) {
        resultSet = null;
        sortOrder = new ArrayList<PropertyWrapper>();
        for (QueryableDatatype qdt : orderColumns) {
            PropertyWrapper prop = baseRow.getPropertyWrapperOf(qdt);
            if (prop == null) {
                throw new IncorrectRowProviderInstanceSuppliedException(baseRow, qdt);
            }
            sortOrder.add(prop);
        }
        return this;
    }

	/**
	 *
	 * @return this DBTableOLD instance
	 */
	public DBTableOLD<E> clearSortOrder() {
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

	/**
	 *
	 * @param allow
	 * @return this DBTableOLD instance.
	 */
	public DBTableOLD<E> setBlankQueryAllowed(boolean allow) {
        this.blankQueryAllowed = allow;
        return this;
    }

    /**
     * Set the query to return rows that match any conditions
     *
     * <p>
     * This means that all permitted*, excluded*, and comparisons are optional
     * for any rows and rows will be returned if they match any of the
     * conditions.
     *
     * <p>
     * The conditions will be connected by OR in the SQL.
     */
    public void setToMatchAnyCondition() {
        this.options.setMatchAnyConditions();
    }

    /**
     * Set the query to only return rows that match all conditions
     *
     * <p>
     * This is the default state
     *
     * <p>
     * This means that all permitted*, excluded*, and comparisons are required
     * for any rows and the conditions will be connected by AND.
     */
    public void setToMatchAllConditions() {
        this.options.setMatchAllConditions();
    }
}
