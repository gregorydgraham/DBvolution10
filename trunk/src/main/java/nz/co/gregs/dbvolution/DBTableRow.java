/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package nz.co.gregs.dbvolution;

import java.beans.BeanInfo;
import java.beans.IntrospectionException;
import java.beans.Introspector;
import java.beans.PropertyDescriptor;
import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;
import nz.co.gregs.dbvolution.annotations.DBTableColumn;
import nz.co.gregs.dbvolution.annotations.DBTableForeignKey;
import nz.co.gregs.dbvolution.annotations.DBTableName;
import nz.co.gregs.dbvolution.annotations.DBTablePrimaryKey;
import nz.co.gregs.dbvolution.databases.DBDatabase;

/**
 *
 * @author gregory.graham
 */
abstract public class DBTableRow {

    private DBDatabase database;

    public DBTableRow() {
    }

    public String getPrimaryKeyValue() throws IntrospectionException, IllegalAccessException, IllegalArgumentException, InvocationTargetException {
        String pkColumnValue = "";
        QueryableDatatype queryableValueOfField;
        @SuppressWarnings("unchecked")
        Class<? extends DBTableRow> thisClass = this.getClass();
        Field[] fields = thisClass.getDeclaredFields();
        for (Field field : fields) {
            if (field.isAnnotationPresent(DBTablePrimaryKey.class)) {
                queryableValueOfField = this.getQueryableValueOfField(field);
                pkColumnValue = queryableValueOfField.toSQLString();
                break;
            }
        }
        if (pkColumnValue.isEmpty()) {
            throw new RuntimeException("Primary Key Field Not Defined: Please define the primary key field using the @DBTablePrimaryKey annotation.");
        } else {
            return pkColumnValue;
        }

    }

    public String getPrimaryKeyName() throws IntrospectionException, IllegalAccessException, IllegalArgumentException, InvocationTargetException {
        String pkColumnValue = "";
        QueryableDatatype queryableValueOfField;
        @SuppressWarnings("unchecked")
        Class<? extends DBTableRow> thisClass = this.getClass();
        Field[] fields = thisClass.getDeclaredFields();
        for (Field field : fields) {
            if (field.isAnnotationPresent(DBTablePrimaryKey.class)) {
                return field.getAnnotation(DBTableColumn.class).value();
            }
        }
        throw new RuntimeException("Primary Key Field Not Defined: Please define the primary key field of "+this.getClass().getSimpleName()+" using the @DBTablePrimaryKey annotation.");
    }

    /**
     * DO NOT USE THIS.
     *
     * @param <Q>
     * @param field
     * @return
     * @throws IntrospectionException
     * @throws IllegalArgumentException
     * @throws InvocationTargetException
     */
    @SuppressWarnings("unchecked")
    public <Q extends QueryableDatatype> Q getQueryableValueOfField(Field field) throws IntrospectionException, IllegalArgumentException, InvocationTargetException {
        BeanInfo info = Introspector.getBeanInfo(this.getClass());
        PropertyDescriptor[] descriptors = info.getPropertyDescriptors();
        for (PropertyDescriptor pd : descriptors) {
            if (pd.getName().equals(field.getName())) {
                Method readMethod = pd.getReadMethod();
                if (readMethod != null) {
                    try {
                        return (Q) readMethod.invoke(this);
                    } catch (IllegalAccessException ex) {
                        throw new RuntimeException("GET Method Found But Unable To Access: Please change GET method to public for " + this.getClass().getSimpleName() + "." + field.getName(), ex);
                    }
                }
            }
        }
        try {
            // no GET method found so try direct method
            return (Q) field.get(this);
            //throw new UnsupportedOperationException("No Appropriate Get Method Found In " + this.getClass().getSimpleName() + " for " + field.toGenericString());
        } catch (IllegalAccessException ex) {
            throw new RuntimeException("Unable To Access Variable Nor GET Method: Please change protection to public for GET method or field " + this.getClass().getSimpleName() + "." + field.getName(), ex);
        }
    }
    
    Map<String, QueryableDatatype> getColumnsAndQueryableDatatypes() throws IntrospectionException, IllegalArgumentException, InvocationTargetException{
        HashMap<String, QueryableDatatype> columnsAndQDTs = new HashMap<String, QueryableDatatype>();
        String columnName;
        QueryableDatatype qdt;
        
        Field[] fields = this.getClass().getDeclaredFields();
        for (Field field : fields) {
            if (field.isAnnotationPresent(DBTableColumn.class)) {
                qdt = this.getQueryableValueOfField(field);
                columnName = getDBColumnName(field);
                columnsAndQDTs.put(columnName, qdt);
            }
        }
        return columnsAndQDTs;
    }

    /**
     *
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
    public String getWhereClause() throws IllegalArgumentException, IllegalAccessException, SQLException, InstantiationException, NoSuchMethodException, InvocationTargetException, ClassNotFoundException, IntrospectionException {
        StringBuilder whereClause = new StringBuilder();
        Field[] fields = this.getClass().getDeclaredFields();
        for (Field field : fields) {
            if (field.isAnnotationPresent(DBTableColumn.class)) {
                QueryableDatatype qdt = this.getQueryableValueOfField(field);
                qdt.setDatabase(this.database);
                whereClause.append(qdt.getWhereClause(this.database.formatTableAndColumnName(this.getTableName(), getDBColumnName(field))));
            }
        }
        return whereClause.toString();
    }

    /**
     * Probably not needed by the programmer, this is the convenience function
     * to find the table name specified by
     *
     * @DBTableName
     *
     * @return the name of the table in the database specified to correlate with
     * the specified type
     *
     */
    public String getTableName() {
        @SuppressWarnings("unchecked")
        Class<? extends DBTableRow> thisClass = (Class<? extends DBTableRow>) this.getClass();
        if (thisClass.isAnnotationPresent(DBTableName.class)) {
            DBTableName annotation = thisClass.getAnnotation(DBTableName.class);
            return annotation.value();
        } else {
            return thisClass.getSimpleName();
        }
    }

    @Override
    public String toString() {
        StringBuilder string = new StringBuilder();
        Class<? extends DBTableRow> thisClass = this.getClass();
        Field[] fields = thisClass.getDeclaredFields();

        String separator = "";

        for (Field field : fields) {
            if (field.isAnnotationPresent(DBTableColumn.class)) {
                try {
                    string.append(separator);
                    string.append(" ");
                    string.append(field.getName());
                    string.append(":");
                    try {
                        string.append(getQueryableValueOfField(field));
                    } catch (IntrospectionException ex) {
                        Logger.getLogger(DBTableRow.class.getName()).log(Level.SEVERE, null, ex);
                    } catch (InvocationTargetException ex) {
                        Logger.getLogger(DBTableRow.class.getName()).log(Level.SEVERE, null, ex);
                    }
                } catch (IllegalArgumentException ex) {
                    Logger.getLogger(DBTableRow.class.getName()).log(Level.SEVERE, null, ex);
                }
                separator = ",";
            }
        }
        return string.toString();
    }

    /**
     * @param database the database to set
     */
    protected void setDatabase(DBDatabase theDatabase) throws IntrospectionException, IllegalArgumentException, InvocationTargetException {
        this.database = theDatabase;

        for (Field field : this.getClass().getDeclaredFields()) {
            if (field.isAnnotationPresent(DBTableColumn.class)) {
                getQueryableValueOfField(field).setDatabase(database);
            }
        }
    }

    protected String getValuesClause() throws IntrospectionException, IllegalArgumentException, InvocationTargetException {
        StringBuilder string = new StringBuilder();
        Class<? extends DBTableRow> thisClass = this.getClass();
        Field[] fields = thisClass.getDeclaredFields();

        String separator = " VALUES ( ";
        for (Field field : fields) {
            if (field.isAnnotationPresent(DBTableColumn.class)) {
                string.append(separator).append(getQueryableValueOfField(field).toSQLString());
                separator = ",";
            }
        }
        return string.append(")").toString();
    }

    /**
     *
     * @return @throws IntrospectionException
     * @throws IllegalArgumentException
     * @throws InvocationTargetException
     */
    protected List<String> getColumnNames() throws IntrospectionException, IllegalArgumentException, InvocationTargetException {
        ArrayList<String> columnNames = new ArrayList<String>();
        Class<? extends DBTableRow> thisClass = this.getClass();
        Field[] fields = thisClass.getDeclaredFields();

        for (Field field : fields) {
            //            if (field.isAnnotationPresent(DBTableColumn.class)) {
            //                DBTableColumn annotation = field.getAnnotation(DBTableColumn.class);
            //                columnNames.add(annotation.value());
            //            }
            String dbColumnName = getDBColumnName(field);
            if (dbColumnName != null) {
                columnNames.add(dbColumnName);
            }
        }
        return columnNames;
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

    protected Map<DBTableForeignKey, DBTableColumn> getForeignKeys() throws IntrospectionException, IllegalArgumentException, InvocationTargetException {
        HashMap<DBTableForeignKey, DBTableColumn> foreignKeys;
        foreignKeys = new HashMap<DBTableForeignKey, DBTableColumn>();
        Class<? extends DBTableRow> thisClass = this.getClass();
        Field[] fields = thisClass.getDeclaredFields();

        for (Field field : fields) {
            if (field.isAnnotationPresent(DBTableForeignKey.class)) {
                DBTableForeignKey annotation = field.getAnnotation(DBTableForeignKey.class);
                DBTableColumn columnName = field.getAnnotation(DBTableColumn.class);
                foreignKeys.put(annotation, columnName);
            }
        }
        return foreignKeys;
    }

    boolean hasPrimaryKeyForFK(DBTableForeignKey fk) throws IntrospectionException, IllegalAccessException, IllegalArgumentException, InvocationTargetException {
        //        final String tableName = getTableName();
        //        final String primaryKeyName = getPrimaryKeyName();
        //        final String properPKName = database.formatTableAndColumnName(tableName, primaryKeyName);
        //        final String fkName = fk.value();
        //        return properPKName.equals(fkName);
        Class fkTableRow = fk.dbTableRow();
        return this.getClass().equals(fkTableRow);
    }
}