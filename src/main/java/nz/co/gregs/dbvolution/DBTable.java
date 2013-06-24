package nz.co.gregs.dbvolution;

import java.beans.BeanInfo;
import java.beans.IntrospectionException;
import java.beans.Introspector;
import java.beans.PropertyDescriptor;
import java.io.PrintStream;
import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.sql.*;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import nz.co.gregs.dbvolution.annotations.DBSelectQuery;
import nz.co.gregs.dbvolution.annotations.DBTableColumn;
import nz.co.gregs.dbvolution.annotations.DBTableForeignKey;
import nz.co.gregs.dbvolution.annotations.DBTablePrimaryKey;
import nz.co.gregs.dbvolution.databases.DBDatabase;

/**
 *
 * @param <E>
 * @author gregory.graham
 */
public class DBTable<E extends DBTableRow> extends java.util.ArrayList<E> implements List<E> {

    private static final long serialVersionUID = 1L;
    private static boolean printSQLBeforeExecuting = false;
    private DBDatabase theDatabase = null;
    E dummy;

    /**
     * With a DBDatabase subclass it's easier
     *
     * @param dummyObject
     * @param myDatabase
     */
    public DBTable(E dummyObject, DBDatabase myDatabase) {
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

        if (field.isAnnotationPresent(DBTableColumn.class)) {
            DBTableColumn annotation = field.getAnnotation(DBTableColumn.class);
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
            allFields.append(separator).append(" ").append(getDBColumnName(field));
            separator = ",";
        }
        return allFields.toString();
    }

    private String getSelectStatement() {
        StringBuilder selectStatement = new StringBuilder();
        DBSelectQuery selectQueryAnnotation = dummy.getClass().getAnnotation(DBSelectQuery.class);
        if (selectQueryAnnotation != null) {
            selectStatement.append(selectQueryAnnotation.value());
        } else {
            selectStatement.append("select ");
            selectStatement.append(getAllFieldsForSelect()).append(" from ").append(dummy.getTableName()).append(";");
        }

        if (printSQLBeforeExecuting) {
            System.out.println(selectStatement);
        }

        return selectStatement.toString();
    }

    private String getSelectStatementForWhereClause() {
        StringBuilder selectStatement = new StringBuilder();
        DBSelectQuery selectQueryAnnotation = dummy.getClass().getAnnotation(DBSelectQuery.class);
        if (selectQueryAnnotation != null) {
            selectStatement.append(selectQueryAnnotation.value()).append(" where 1=1 ");
        } else {
            selectStatement.append("select ");

            selectStatement.append(getAllFieldsForSelect()).append(" from ").append(dummy.getTableName()).append(" where 1=1 ");
        }
//        if (printSQLBeforeExecuting){
//            System.out.println(selectStatement);
//        }
        return selectStatement.toString();
    }

    /**
     * Not used externally but used to create instances of the TableRow subclass
     *
     * @return
     * @throws InstantiationException
     * @throws IllegalAccessException
     * @throws NoSuchMethodException
     * @throws IllegalArgumentException
     * @throws InvocationTargetException
     */
    @SuppressWarnings("unchecked")
    protected E createObject() throws InstantiationException, IllegalAccessException, NoSuchMethodException, IllegalArgumentException, InvocationTargetException {
        return (E) dummy.getClass().getConstructor().newInstance();
    }

    /**
     * Use this carefully as it does what it says on the label: Gets All Rows of
     * the table from the database. If your database has umpteen gazillion rows
     * in the VeryBig table and you call this, don't come crying to me.
     *
     * @throws SQLException
     * @throws InstantiationException
     * @throws IllegalAccessException
     * @throws NoSuchMethodException
     * @throws IllegalArgumentException
     * @throws InvocationTargetException
     * @throws IntrospectionException
     */
    public void getAllRows() throws SQLException, InstantiationException, IllegalArgumentException, IllegalAccessException, NoSuchMethodException, InvocationTargetException, IntrospectionException //throws SQLException, InstantiationException, IllegalAccessException, NoSuchMethodException, IllegalArgumentException, InvocationTargetException, ClassNotFoundException 
    {
        this.clear();

        String selectStatement = this.getSelectStatement();

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
    }

    private void addAllFields(DBTable<E> dbTable, ResultSet resultSet) throws SQLException, InstantiationException, IllegalArgumentException, IllegalAccessException, NoSuchMethodException, InvocationTargetException, IntrospectionException {
        ResultSetMetaData rsMeta = resultSet.getMetaData();
        Map<String, Integer> dbColumnNames = new HashMap<String, Integer>();
        for (int k = 1; k <= rsMeta.getColumnCount(); k++) {
            dbColumnNames.put(rsMeta.getColumnName(k), k);
        }

        while (resultSet.next()) {
            @SuppressWarnings("unchecked")
            E tableRow = createObject();

            Field[] fields = tableRow.getClass().getDeclaredFields();



            for (Field field : fields) {
                if (field.isAnnotationPresent(DBTableColumn.class)) {
                    String dbColumnName = getDBColumnName(field);
                    int dbColumnIndex = dbColumnNames.get(theDatabase.formatColumnName(dbColumnName));

                    setObjectFieldValueToColumnValue(rsMeta, dbColumnIndex, field, tableRow, resultSet, dbColumnName);
                }
            }
            dbTable.add(tableRow);
        }
    }

    private void setObjectFieldValueToColumnValue(ResultSetMetaData rsMeta, int dbColumnIndex, Field field, DBTableRow tableRow, ResultSet resultSet, String dbColumnName) throws SQLException, IllegalArgumentException, IllegalAccessException, IntrospectionException, InvocationTargetException {
        QueryableDatatype qdt = getQueryableDatatypeOfField(tableRow, field);
        int columnType = rsMeta.getColumnType(dbColumnIndex);
        switch (columnType) {
            case Types.INTEGER:
            case Types.BIGINT:
            case Types.BINARY:
            case Types.BOOLEAN:
            case Types.ROWID:
            case Types.SMALLINT:
                Long aLong = resultSet.getLong(dbColumnName);
                qdt.isLiterally(aLong);
                break;
            case Types.DECIMAL:
            case Types.DOUBLE:
            case Types.FLOAT:
            case Types.NUMERIC:
            case Types.REAL:
                Double aDouble = resultSet.getDouble(dbColumnName);
                qdt.isLiterally(aDouble);
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
                qdt.isLiterally(string);
                break;
            case Types.DATE:
            case Types.TIME:
                Date date = resultSet.getDate(dbColumnName);
                qdt.isLiterally(date);
                break;
            case Types.TIMESTAMP:
                Timestamp timestamp = resultSet.getTimestamp(dbColumnName);
                qdt.isLiterally(timestamp);
                break;
            case Types.VARBINARY:
            case Types.JAVA_OBJECT:
            case Types.LONGVARBINARY:
                Object obj = resultSet.getObject(dbColumnName);
                qdt.isLiterally(obj);
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
            if (field.isAnnotationPresent(DBTablePrimaryKey.class)) {
                pkColumn = this.getDBColumnName(field);
            }
        }
        if (pkColumn.isEmpty()) {
            throw new RuntimeException("Primary Key Field Not Defined: Please define the primary key field of "+thisClass.getSimpleName()+"using the @DBTablePrimaryKey annotation.");
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

    private DBTable<E> getRows(String whereClause) throws SQLException, InstantiationException, IllegalAccessException, NoSuchMethodException, IllegalArgumentException, InvocationTargetException, ClassNotFoundException, IntrospectionException {
        this.clear();
        String selectStatement = this.getSelectStatementForWhereClause() + whereClause + ";";
        if (printSQLBeforeExecuting) {
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
     * @throws InstantiationException
     * @throws IllegalAccessException
     * @throws NoSuchMethodException
     * @throws IllegalArgumentException
     * @throws InvocationTargetException
     * @throws SQLException
     * @throws ClassNotFoundException
     * @throws IntrospectionException
     */
    public DBTable<E> getByPrimaryKey(Object pkValue) throws InstantiationException, IllegalAccessException, NoSuchMethodException, IllegalArgumentException, InvocationTargetException, SQLException, ClassNotFoundException, IntrospectionException {
        String whereClause = " and " + getPrimaryKeyColumn() + " = '" + escapeSingleQuotes(pkValue.toString()) + "'";
        this.getRows(whereClause);
        return this;
    }

    public DBTable<E> getByPrimaryKey(Number pkValue) throws InstantiationException, IllegalAccessException, NoSuchMethodException, IllegalArgumentException, InvocationTargetException, SQLException, ClassNotFoundException, IntrospectionException {
        String whereClause = " and " + getPrimaryKeyColumn() + " = " + pkValue + " ";
        this.getRows(whereClause);
        return this;
    }

    public DBTable<E> getByPrimaryKey(Date pkValue) throws InstantiationException, IllegalAccessException, NoSuchMethodException, IllegalArgumentException, InvocationTargetException, SQLException, ClassNotFoundException, IntrospectionException {
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
     * @throws IllegalArgumentException
     * @throws IllegalAccessException
     * @throws SQLException
     * @throws InstantiationException
     * @throws NoSuchMethodException
     * @throws InvocationTargetException
     * @throws ClassNotFoundException
     * @throws IntrospectionException
     */
    public DBTable<E> getByExample(E queryTemplate) throws IllegalArgumentException, IllegalAccessException, SQLException, InstantiationException, NoSuchMethodException, InvocationTargetException, ClassNotFoundException, IntrospectionException {
        return getRows(getSQLForExample(queryTemplate));
    }

    /**
     * Returns the WHERE clause used by the getByExample method. Provided to aid
     * understanding and debugging.
     *
     * @param query
     * @return
     * @throws IllegalArgumentException
     * @throws IllegalAccessException
     * @throws SQLException
     * @throws InstantiationException
     * @throws NoSuchMethodException
     * @throws InvocationTargetException
     * @throws ClassNotFoundException
     * @throws IntrospectionException
     */
    public String getSQLForExample(E query) throws IllegalArgumentException, IllegalAccessException, SQLException, InstantiationException, NoSuchMethodException, InvocationTargetException, ClassNotFoundException, IntrospectionException {
//        StringBuilder whereClause = new StringBuilder();
//        Field[] fields = query.getClass().getDeclaredFields();
//        for (Field field : fields) {
//            if (field.isAnnotationPresent(DBTableColumn.class)) {
//                QueryableDatatype qdt = query.getQueryableValueOfField(field);
//                qdt.setDatabase(theDatabase);
//                whereClause.append(qdt.getWhereClause(getDBColumnName(field)));
//            }
//        }
//        return whereClause.toString();
        query.setDatabase(theDatabase);
        return query.getWhereClause();
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
     * @throws IllegalArgumentException
     * @throws IllegalAccessException
     * @throws SQLException
     * @throws InstantiationException
     * @throws NoSuchMethodException
     * @throws InvocationTargetException
     * @throws ClassNotFoundException
     * @throws IntrospectionException
     */
    public DBTable<E> getByRawSQL(String sqlWhereClause) throws IllegalArgumentException, IllegalAccessException, SQLException, InstantiationException, NoSuchMethodException, InvocationTargetException, ClassNotFoundException, IntrospectionException {
        if (sqlWhereClause.toLowerCase().matches("^\\s*and\\s+.*")) {
            return getRows(sqlWhereClause.replaceAll("\\s*;\\s*$", ""));
        } else {
            return getRows(" AND " + sqlWhereClause.replaceAll("\\s*;\\s*$", ""));
        }
    }

    /**
     * Convenience method to print all the rows in the current collection
     * Equivalent to: for (E row : this) { System.out.println(row); }
     *
     */
    public void printAllRows() {
        for (E row : this) {
            System.out.println(row);
        }
    }

    /**
     * the same as printAllRows but allows you to specify the PrintStream
     * required
     *
     * myTable.printAllRows(System.err);
     *
     * @param ps
     */
    public void printAllRows(PrintStream ps) {
        for (E row : this) {
            ps.println(row);
        }
    }

    private QueryableDatatype getQueryableDatatypeOfField(DBTableRow tableRow, Field field) throws IntrospectionException, InvocationTargetException, IllegalArgumentException, IllegalAccessException {
        QueryableDatatype qdt = null;
        BeanInfo info = Introspector.getBeanInfo(tableRow.getClass());
        PropertyDescriptor[] descriptors = info.getPropertyDescriptors();
        String fieldName = field.getName();
        for (PropertyDescriptor pd : descriptors) {
            String pdName = pd.getName();
            if (pdName.equals(fieldName)) {
                try {
                    Method readMethod = pd.getReadMethod();
                    if (readMethod == null) {
                        Object possQDT = field.get(tableRow);
                        if (possQDT instanceof QueryableDatatype) {
                            return (QueryableDatatype) possQDT;
                        } else {
                            throw new RuntimeException("Unable To Access Read Method for \"" + field.getName() + "\" in class " + tableRow.getClass().getSimpleName());
                        }
                    } else {
                        Object fieldQDT = readMethod.invoke(tableRow);
                        if (fieldQDT instanceof QueryableDatatype) {
                            qdt = (QueryableDatatype) fieldQDT;
                            qdt.setDatabase(this.theDatabase);
                        }
                    }
                    break;
                } catch (IllegalAccessException illacc) {
                    throw new RuntimeException("Could Not Access SET Method for " + tableRow.getClass().getSimpleName() + "." + field.getName() + ": Please ensure the SET method is public: " + tableRow.getClass().getSimpleName() + "." + field.getName());
                }
            }
        }

        if (qdt == null) {
            Object possQDT = field.get(tableRow);
            if (possQDT instanceof QueryableDatatype) {
                return (QueryableDatatype) possQDT;
            } else {
                throw new RuntimeException("Unable Access QueryDatatype for \"" + field.getName() + "\" in class " + tableRow.getClass().getSimpleName());
            }
        }

        return qdt;
    }

    /**
     *
     * Returns the first row of the table, particularly helpful when you know
     * there is only one row
     *
     * @return
     */
    public E firstRow() {
        if (this.size() > 0) {
            return this.get(0);
        } else {
            return null;
        }
    }

    /**
     *
     * Synonym for firstRow()
     *
     * @return
     */
    public final E firstElement() {
        return this.firstRow();
    }

    public void insert(E newRow) throws IntrospectionException, IllegalArgumentException, InvocationTargetException, SQLException {
        ArrayList<E> arrayList = new ArrayList<E>();
        arrayList.add(newRow);
        insert(arrayList);
    }

    public void insert(List<E> newRows) throws IntrospectionException, IllegalArgumentException, InvocationTargetException, SQLException {
        Statement statement = theDatabase.getDBStatement();
        StringBuilder sqlInsert = new StringBuilder();
        for (E row : newRows) {
            row.setDatabase(theDatabase);
            String sql = "INSERT INTO " + row.getTableName() + "(" + this.getAllFieldsForSelect() + ") " + row.getValuesClause() + ";";
            if (printSQLBeforeExecuting) {
                System.out.println(sql);
            }
            statement.addBatch(sql);
        }
        statement.executeBatch();
        statement.getConnection().commit();
    }

    public void delete(E oldRow) throws IntrospectionException, IllegalArgumentException, InvocationTargetException, SQLException, IllegalAccessException {
        ArrayList<E> arrayList = new ArrayList<E>();
        arrayList.add(oldRow);
        delete(arrayList);
    }

    public void delete(List<E> oldRows) throws IntrospectionException, IllegalArgumentException, InvocationTargetException, SQLException, IllegalAccessException {
        Statement statement = theDatabase.getDBStatement();
        StringBuilder sqlInsert = new StringBuilder();
        for (E row : oldRows) {
            row.setDatabase(theDatabase);
            String sql = "DELETE FROM " + row.getTableName() + " WHERE " + this.getPrimaryKeyColumn() + " = " + row.getPrimaryKeyValue() + ";";
            if (printSQLBeforeExecuting) {
                System.out.println(sql);
            }
            statement.addBatch(sql);
        }
        statement.executeBatch();
        statement.getConnection().commit();
    }

    public String getTableName() {
        return this.dummy.getTableName();
    }

    List<String> getColumnNames() throws IntrospectionException, IllegalArgumentException, InvocationTargetException {
        return dummy.getColumnNames();
    }

    Map<DBTableForeignKey, DBTableColumn> getForeignKeys() throws IntrospectionException, IllegalArgumentException, InvocationTargetException {
        return dummy.getForeignKeys();
    }

    String getPrimaryKeyName() throws IntrospectionException, IllegalAccessException, IllegalArgumentException, InvocationTargetException {
        return dummy.getPrimaryKeyValue();
    }
}
