package nz.co.gregs.dbvolution;

import nz.co.gregs.dbvolution.annotations.DBTablePrimaryKey;
import nz.co.gregs.dbvolution.annotations.DBTableName;
import nz.co.gregs.dbvolution.annotations.DBTableColumn;
import java.beans.BeanInfo;
import java.beans.IntrospectionException;
import java.beans.Introspector;
import java.beans.PropertyDescriptor;
import java.io.PrintStream;
import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.sql.*;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;
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

    /**
     * Probably not needed by the programmer, this is the convenience function
     * to find the table name specified by
     *
     * @DBTableName
     *
     * @return the name of the table in the database specified to correlate with
     * the specified type
     */
    public String getTableName() {
        @SuppressWarnings("unchecked")
        Class<E> thisClass = (Class<E>) dummy.getClass();
        if (thisClass.isAnnotationPresent(DBTableName.class)) {
            DBTableName annotation = thisClass.getAnnotation(DBTableName.class);
            return annotation.value();
        } else {
            return thisClass.getSimpleName();
        }
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
        selectStatement.append("select ");
        
        selectStatement.append(getAllFieldsForSelect()).append(" from ").append(getTableName()).append(";");
        
        if (printSQLBeforeExecuting) {
            System.out.println(selectStatement);
        }
        
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
        
        String selectStatement = this.getSelectStatement();
//        if (printSQLBeforeExecuting) {
//            System.out.println(selectStatement);
//        }
        java.sql.Connection connection;
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
                    int dbColumnIndex = dbColumnNames.get(dbColumnName);
                    
                    setObjectFieldValueToColumnValue(rsMeta, dbColumnIndex, field, tableRow, resultSet, dbColumnName);
                }
            }
            dbTable.add(tableRow);
        }
    }
    
    private void setObjectFieldValueToColumnValue(ResultSetMetaData rsMeta, int dbColumnIndex, Field field, DBTableRow tableRow, ResultSet resultSet, String dbColumnName) throws SQLException, IllegalArgumentException, IllegalAccessException, IntrospectionException, InvocationTargetException {
        //TODO: check column type and field class are compatible
        Object value = null;
        int columnType = rsMeta.getColumnType(dbColumnIndex);
        switch (columnType) {
            case Types.INTEGER:
            case Types.BIGINT:
            case Types.BINARY:
            case Types.BOOLEAN:
            case Types.ROWID:
            case Types.SMALLINT:
                value = new DBInteger(resultSet.getLong(dbColumnName));
                break;
            case Types.DECIMAL:
            case Types.DOUBLE:
            case Types.FLOAT:
            case Types.NUMERIC:
            case Types.REAL:
                value = new DBNumber(resultSet.getDouble(dbColumnName));
                break;
            case Types.VARCHAR:
            case Types.CHAR:
            case Types.NCHAR:
            case Types.NVARCHAR:
            case Types.CLOB:
            case Types.NCLOB:
            case Types.LONGNVARCHAR:
            case Types.LONGVARCHAR:
                value = new DBString(resultSet.getString(dbColumnName));
                break;
            case Types.DATE:
            case Types.TIME:
                value = new DBDate(resultSet.getDate(dbColumnName));
                break;
            case Types.TIMESTAMP:
                value = new DBDate(resultSet.getTimestamp(dbColumnName));
                break;
            case Types.VARBINARY:
            case Types.JAVA_OBJECT:
            case Types.LONGVARBINARY:
                value = new DBBlob(resultSet.getObject(dbColumnName));
                break;
            default:
                throw new RuntimeException("Unknown Java SQL Type: " + rsMeta.getColumnType(dbColumnIndex));
        }
        setValueOfField(tableRow, field, value);
        
        
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
            throw new RuntimeException("Primary Key Field Not Defined: Please define the primay key field using the @DBTablePrimaryKey annotation.");
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
    
    private String getSelectStatementForWhereClause() {
        StringBuilder selectStatement = new StringBuilder();
        selectStatement.append("select ");
        
        selectStatement.append(getAllFieldsForSelect()).append(" from ").append(getTableName()).append(" where 1=1 ");

//        if (printSQLBeforeExecuting){
//            System.out.println(selectStatement);
//        }
        return selectStatement.toString();
    }
    
    private DBTable<E> getRows(String whereClause) throws SQLException, InstantiationException, IllegalAccessException, NoSuchMethodException, IllegalArgumentException, InvocationTargetException, ClassNotFoundException, IntrospectionException {
        DBTable<E> dbTable = new DBTable<E>(dummy, theDatabase);
        String selectStatement = dbTable.getSelectStatementForWhereClause() + whereClause + ";";
        if (printSQLBeforeExecuting) {
            System.out.println(selectStatement);
        }
        
        Statement statement = theDatabase.getDBStatement();
        boolean executed = statement.execute(selectStatement);
        ResultSet resultSet = statement.getResultSet();
        
        addAllFields(dbTable, resultSet);
        return dbTable;
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
        String whereClause = "and " + getPrimaryKeyColumn() + " = '" + escapeSingleQuotes(pkValue.toString()) + "'";
        DBTable<E> table = this.getRows(whereClause);
        return table;
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
        StringBuilder whereClause = new StringBuilder();
        Field[] fields = query.getClass().getDeclaredFields();
        for (Field field : fields) {
            if (field.isAnnotationPresent(DBTableColumn.class)) {
                QueryableDatatype fieldValue = query.getQueryableValueOfField(field);
                fieldValue.setDatabase(theDatabase);
                //if (fieldValue.getClass().getSuperclass() == QueryableDatatype.class) {
                QueryableDatatype qdt = fieldValue;
                whereClause.append(qdt.getWhereClause(getDBColumnName(field)));
                //}
            }
        }
        return whereClause.toString();
    }

    /**
     * For the particularly hard queries, just provide the actual WHERE clause
     * you want to use.
     *
     * myExample.getLanguage.useLikeComparison("%JAVA%"); is similar to:
     * getByRawSQL("and language like '%JAVA%'");
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
    
    private Object setValueOfField(DBTableRow tableRow, Field field, Object value) throws IntrospectionException, InvocationTargetException, IllegalArgumentException, IllegalAccessException {
        BeanInfo info = Introspector.getBeanInfo(tableRow.getClass());
        PropertyDescriptor[] descriptors = info.getPropertyDescriptors();
        String fieldName = field.getName();
        for (PropertyDescriptor pd : descriptors) {
            String pdName = pd.getName();
            if (pdName.equals(fieldName)) {
                try {
                    pd.getWriteMethod().invoke(tableRow, value);
                    Object invoke = pd.getReadMethod().invoke(tableRow);
                    if (invoke instanceof QueryableDatatype) {
                        QueryableDatatype qdt = (QueryableDatatype) invoke;
                        qdt.setDatabase(this.theDatabase);
                    }
                    return tableRow;
                } catch (IllegalAccessException illacc) {
                    throw new RuntimeException("Could Not Access SET Method for " + tableRow.getClass().getSimpleName() + "." + field.getName() + ": Please ensure the SET method is public: " + tableRow.getClass().getSimpleName() + "." + field.getName());
                } catch (IllegalArgumentException illarg) {
                    throw new RuntimeException("Field: " + field.getName() + " is the wrong type for the database value: Field.type: " + field.toGenericString() + " Column.type:" + value.getClass().getSimpleName(), illarg);
                }
            }
        }
        //try {
        // didn't find a set method so look for a public variable
        field.set(tableRow, value);
//        } catch (IllegalAccessException ex) {
//            throw new RuntimeException("No Means Of Accessing " + tableRow.getClass().getSimpleName() + "." + field.getName() + " Was Found: Please ensure the field is public, or there is a public SET method for it: " + tableRow.getClass().getSimpleName() + "." + field.getName());
//        }

        return tableRow;
        //throw new UnsupportedOperationException("No Appropriate Set Method Found In " + tableRow.getClass().getSimpleName() + " for " + field.toGenericString());
    }
    
    public E firstRow() {
        if (this.size() > 1) {
            return this.get(0);
        } else {
            return null;
        }
    }
    
    public final E firstElement() {
        return this.firstRow();
    }
}
