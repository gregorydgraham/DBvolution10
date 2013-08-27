/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package nz.co.gregs.dbvolution;

import java.beans.BeanInfo;
import java.beans.IntrospectionException;
import java.beans.Introspector;
import java.beans.PropertyDescriptor;
import java.io.Serializable;
import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import nz.co.gregs.dbvolution.actions.DBActionList;
import nz.co.gregs.dbvolution.actions.DBSaveBLOB;
import nz.co.gregs.dbvolution.annotations.DBColumn;
import nz.co.gregs.dbvolution.annotations.DBForeignKey;
import nz.co.gregs.dbvolution.annotations.DBTableName;
import nz.co.gregs.dbvolution.annotations.DBPrimaryKey;
import nz.co.gregs.dbvolution.databases.DBDatabase;
import nz.co.gregs.dbvolution.databases.definitions.DBDefinition;
import nz.co.gregs.dbvolution.exceptions.IncorrectDBRowInstanceSuppliedException;
import nz.co.gregs.dbvolution.operators.DBOperator;

/**
 *
 * @author gregory.graham
 */
abstract public class DBRow implements Serializable {

    private transient DBDatabase database;
    private List<Field> ignoredRelationships = new ArrayList<Field>();
    private List<Field> returnColumns = new ArrayList<Field>();
    private final List<Field> fkFields = new ArrayList<Field>();
    private List<DBRelationship> adHocRelationships = new ArrayList<DBRelationship>();
    private Field primaryKeyField;
    private HashMap<String, QueryableDatatype> columnsAndQDTs;
    private Boolean hasBlobs;
    private List<DBLargeObject> blobColumns = new ArrayList<DBLargeObject>();

    public DBRow() {
    }

    protected static <T extends DBRow> T getDBRow(Class<T> requiredDBRowClass) {
        try {
            return requiredDBRowClass.getConstructor().newInstance();
        } catch (NoSuchMethodException ex) {
            throw new RuntimeException("Unable To Create " + requiredDBRowClass.getClass().getSimpleName() + ": Please ensure that the constructor of  " + requiredDBRowClass.getClass().getSimpleName() + " has no arguments, throws no exceptions, and is public", ex);
        } catch (SecurityException ex) {
            throw new RuntimeException("Unable To Create " + requiredDBRowClass.getClass().getSimpleName() + ": Please ensure that the constructor of  " + requiredDBRowClass.getClass().getSimpleName() + " has no arguments, throws no exceptions, and is public", ex);
        } catch (InstantiationException ex) {
            throw new RuntimeException("Unable To Create " + requiredDBRowClass.getClass().getSimpleName() + ": Please ensure that the constructor of  " + requiredDBRowClass.getClass().getSimpleName() + " has no arguments, throws no exceptions, and is public", ex);
        } catch (IllegalAccessException ex) {
            throw new RuntimeException("Unable To Create " + requiredDBRowClass.getClass().getSimpleName() + ": Please ensure that the constructor of  " + requiredDBRowClass.getClass().getSimpleName() + " has no arguments, throws no exceptions, and is public", ex);
        } catch (IllegalArgumentException ex) {
            throw new RuntimeException("Unable To Create " + requiredDBRowClass.getClass().getSimpleName() + ": Please ensure that the constructor of  " + requiredDBRowClass.getClass().getSimpleName() + " has no arguments, throws no exceptions, and is public", ex);
        } catch (InvocationTargetException ex) {
            throw new RuntimeException("Unable To Create " + requiredDBRowClass.getClass().getSimpleName() + ": Please ensure that the constructor of  " + requiredDBRowClass.getClass().getSimpleName() + " has no arguments, throws no exceptions, and is public", ex);
        }
    }

    public QueryableDatatype getPrimaryKey() {
        QueryableDatatype queryableValueOfField = this.getQueryableValueOfField(getPrimaryKeyField());
        return queryableValueOfField;
    }

    @Deprecated
    public String getPrimaryKeySQLStringValue(DBDatabase db) {
        this.setDatabase(db);
        QueryableDatatype queryableValueOfField;
        queryableValueOfField = this.getQueryableValueOfField(getPrimaryKeyField());
        String pkColumnValue;
        if (queryableValueOfField.hasChanged()) {
            pkColumnValue = queryableValueOfField.getPreviousSQLValue();
        } else {
            pkColumnValue = queryableValueOfField.toSQLString();
        }
        return pkColumnValue;
    }

    public String getPrimaryKeyName() {
        final Field pkField = getPrimaryKeyField();
        if (pkField == null) {
            return null;
        } else {
            return pkField.getAnnotation(DBColumn.class).value();
        }
    }

    private Field getPrimaryKeyField() {
        if (primaryKeyField != null) {
            return primaryKeyField;
        } else {
            Class<? extends DBRow> thisClass = this.getClass();
            Field[] fields = thisClass.getDeclaredFields();
            for (Field field : fields) {
                if (field.isAnnotationPresent(DBPrimaryKey.class)) {
                    return field;
                }
            }
            return null;
//            throw new RuntimeException("Primary Key Field Not Defined: Please define the primary key field of " + this.getClass().getSimpleName() + " using the @DBPrimaryKey annotation.");
        }
    }

    /**
     * DO NOT USE THIS.
     *
     * @param <Q>
     * @param field
     * @return
     */
    @SuppressWarnings("unchecked")
    public <Q extends QueryableDatatype> Q getQueryableValueOfField(Field field) {
        if (field == null) {
            return null;
        }
        Q qdt;
        BeanInfo info = null;
        try {
            info = Introspector.getBeanInfo(this.getClass());
        } catch (IntrospectionException ex) {
            throw new RuntimeException("Unable Retrieve Bean Information: Bean Information Not Found For Class: " + this.getClass().getSimpleName(), ex);
        }
        PropertyDescriptor[] descriptors = info.getPropertyDescriptors();
        for (PropertyDescriptor pd : descriptors) {
            if (pd.getName().equals(field.getName())) {
                Method readMethod = pd.getReadMethod();
                if (readMethod != null) {
                    try {
                        qdt = (Q) readMethod.invoke(this);
                        if (qdt == null) {
                            qdt = QueryableDatatype.getQueryableDatatypeInstance((Class<Q>) pd.getPropertyType());
                            Method setMethod = pd.getWriteMethod();
                            setMethod.invoke(this, qdt);
                        }
                    } catch (IllegalAccessException ex) {
                        throw new RuntimeException("GET Method Found But Unable To Access: Please change GET method to public for " + this.getClass().getSimpleName() + "." + field.getName(), ex);
                    } catch (IllegalArgumentException ex) {
                        throw new RuntimeException("GET Method Found But Somehow The Argument Was Illegal: Please ensure the read method of " + this.getClass().getSimpleName() + "." + field.getName() + "  has NO arguments.", ex.getCause());
                    } catch (InvocationTargetException ex) {
                        throw new RuntimeException("GET Method Found But Unable To Access: Please ensure the read method of " + this.getClass().getSimpleName() + "." + field.getName() + "  has NO arguments.", ex.getCause());
                    }
                }
            }
        }
        try {
            // no GET method found so try direct method
            qdt = (Q) field.get(this);
            if (qdt == null) {
                qdt = QueryableDatatype.getQueryableDatatypeInstance((Class<Q>) field.getType());
                field.set(this, qdt);
            }
        } catch (IllegalAccessException ex) {
            throw new RuntimeException("Unable To Access Variable Nor GET Method: Please change protection to public for GET method or field " + this.getClass().getSimpleName() + "." + field.getName(), ex);
        }
        return qdt;
    }

    Map<String, QueryableDatatype> getColumnsAndQueryableDatatypes() {
        if (columnsAndQDTs == null) {
            columnsAndQDTs = new HashMap<String, QueryableDatatype>();
            String columnName;
            QueryableDatatype qdt;

            Field[] fields = this.getClass().getDeclaredFields();
            for (Field field : fields) {
                if (field.isAnnotationPresent(DBColumn.class)) {
                    qdt = this.getQueryableValueOfField(field);
                    columnName = getDBColumnName(field);
                    columnsAndQDTs.put(columnName, qdt);
                }
            }
        }
        return columnsAndQDTs;
    }

    public List<QueryableDatatype> getQueryableDatatypes() {
        List<QueryableDatatype> arrayList = new ArrayList<QueryableDatatype>();
        arrayList.addAll(getColumnsAndQueryableDatatypes().values());
        return arrayList;
    }

    /**
     *
     * @return
     *
     */
    public String getWhereClause(DBDatabase db) {
        this.setDatabase(db);
        DBDefinition defn = database.getDefinition();
        StringBuilder whereClause = new StringBuilder();
        Field[] fields = this.getClass().getDeclaredFields();
        for (Field field : fields) {
            if (field.isAnnotationPresent(DBColumn.class)) {
                QueryableDatatype qdt = this.getQueryableValueOfField(field);
                qdt.setDatabase(this.database);
                whereClause.append(qdt.getWhereClause(defn.formatTableAndColumnName(this.getTableName(), getDBColumnName(field))));
            }
        }
        return whereClause.toString();
    }

    /**
     *
     * @param db
     * @return true if this DBRow instance has no specified criteria and will
     * create a blank query returning the whole table.
     *
     */
    public boolean willCreateBlankQuery(DBDatabase db) {
        String whereClause = getWhereClause(db);
        if (whereClause == null || whereClause.isEmpty()) {
            return true;
        } else {
            return false;
        }
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
        Class<? extends DBRow> thisClass = (Class<? extends DBRow>) this.getClass();
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
        Class<? extends DBRow> thisClass = this.getClass();
        Field[] fields = thisClass.getDeclaredFields();

        String separator = "";

        for (Field field : fields) {
            if (field.isAnnotationPresent(DBColumn.class)) {
                string.append(separator);
                string.append(" ");
                string.append(field.getName());
                string.append(":");
                string.append(getQueryableValueOfField(field));
                separator = ",";
            }
        }
        return string.toString();
    }

    public String toStringMinusFKs() {
        StringBuilder string = new StringBuilder();
        Class<? extends DBRow> thisClass = this.getClass();
        Field[] fields = thisClass.getDeclaredFields();

        String separator = "";

        for (Field field : fields) {
            if (field.isAnnotationPresent(DBColumn.class)) {
                if (!field.isAnnotationPresent(DBForeignKey.class)) {
                    string.append(separator);
                    string.append(" ");
                    string.append(field.getName());
                    string.append(":");
                    string.append(getQueryableValueOfField(field));
                    separator = ",";
                }
            }
        }
        return string.toString();
    }

    /**
     * @param database the database to set
     */
    protected void setDatabase(DBDatabase theDatabase) {
        this.database = theDatabase;

        for (Field field : this.getClass().getDeclaredFields()) {
            if (field.isAnnotationPresent(DBColumn.class)) {
                getQueryableValueOfField(field).setDatabase(database);
            }
        }
    }

    protected String getValuesClause(DBDatabase db) {
        this.setDatabase(db);
        StringBuilder string = new StringBuilder();
        Class<? extends DBRow> thisClass = this.getClass();
        Field[] fields = thisClass.getDeclaredFields();

        String separator = " VALUES ( ";
        for (Field field : fields) {
            final String fieldTypeName = field.getType().getSimpleName();
            if (field.isAnnotationPresent(DBColumn.class)
                    && !fieldTypeName.equals(DBLargeObject.class.getSimpleName())) {
                final QueryableDatatype qdt = getQueryableValueOfField(field);
                string.append(separator).append(qdt.toSQLString());
                separator = ",";
            }
        }
        return string.append(")").toString();
    }

    protected String getSetClause(DBDatabase db) {
        this.setDatabase(db);
        DBDefinition defn = database.getDefinition();
        StringBuilder sql = new StringBuilder();
        Class<? extends DBRow> thisClass = this.getClass();
        Field[] fields = thisClass.getDeclaredFields();

        String separator = defn.getStartingSetSubClauseSeparator();
        for (Field field : fields) {
            if (field.isAnnotationPresent(DBColumn.class)) {
                final QueryableDatatype qdt = getQueryableValueOfField(field);
                if (qdt.hasChanged()) {
                    String columnName = getDBColumnName(field);
                    sql.append(separator)
                            .append(defn.formatColumnName(columnName))
                            .append(defn.getEqualsComparator())
                            .append(qdt
                            .toSQLString());
                    separator = defn.getSubsequentSetSubClauseSeparator();
                }
            }
        }
        return sql.toString();
    }

    /**
     *
     * @return
     */
    protected List<String> getColumnNames() {
        ArrayList<String> columnNames = new ArrayList<String>();
        Class<? extends DBRow> thisClass = this.getClass();
        Field[] fields = thisClass.getDeclaredFields();

        for (Field field : fields) {
            if (field.isAnnotationPresent(DBColumn.class)) {
                if (returnColumns == null || returnColumns.isEmpty() || returnColumns.contains(field)) {
                    String dbColumnName = getDBColumnName(field);
                    if (dbColumnName != null) {
                        columnNames.add(dbColumnName);
                    }
                }
            }
        }
        return columnNames;
    }

    public String getDBColumnName(QueryableDatatype qdt) {
        return getDBColumnName(this.getFieldOf(qdt));
    }

    public String getDBColumnName(Field field) {
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

    protected static String getTableAndColumnName(DBRow[] baseRows, QueryableDatatype qdt) {
        String columnName;
        String tableName;
        String fullName = null;
        for (DBRow row : baseRows) {
            tableName = row.getTableName();
            columnName = row.getDBColumnName(qdt);
            if (columnName != null) {
                fullName = row.database.getDefinition().formatTableAndColumnName(tableName, columnName);
                return fullName;
            }
        }
        return fullName;
    }

    protected Map<DBForeignKey, DBColumn> getForeignKeys() {
        HashMap<DBForeignKey, DBColumn> foreignKeys;
        foreignKeys = new HashMap<DBForeignKey, DBColumn>();
//        Class<? extends DBRow> thisClass = this.getClass();
        List<Field> fields = this.getForeignKeyFields();

        for (Field field : fields) {
            if (!ignoredRelationships.contains(field)) {
                DBForeignKey annotation = field.getAnnotation(DBForeignKey.class);
                DBColumn columnName = field.getAnnotation(DBColumn.class);
                foreignKeys.put(annotation, columnName);
            }
        }
        return foreignKeys;
    }

    protected List<Field> getForeignKeyFields() {
        if (fkFields.isEmpty()) {
            Class<? extends DBRow> thisClass = this.getClass();
            Field[] fields = thisClass.getDeclaredFields();

            for (Field field : fields) {
                if (field.isAnnotationPresent(DBForeignKey.class)) {
                    fkFields.add(field);
                }
            }
        }
        return fkFields;
    }

    public Field getFieldOf(QueryableDatatype qdt) {
        Field fieldReqd = null;

        Field[] fields = this.getClass().getDeclaredFields();

        for (Field field : fields) {
            final Object fieldOfThisInstance;
            try {
                Object testObject = field.get(this);
                if (testObject == null) {
                    @SuppressWarnings("unchecked")
                    final Class<QueryableDatatype> fieldType = (Class<QueryableDatatype>) field.getType();
                    QueryableDatatype newQdt = QueryableDatatype.getQueryableDatatypeInstance(fieldType);
                    field.set(this, newQdt);
                }
                fieldOfThisInstance = field.get(this);
            } catch (IllegalArgumentException ex) {
                throw new RuntimeException("Field Found But Somehow The Argument Was Illegal: Please ensure the fields of " + this.getClass().getSimpleName() + "." + field.getName() + "  are public.", ex.getCause());
            } catch (IllegalAccessException ex) {
                throw new RuntimeException("Field Found But Unable To Access: Please ensure the fields of " + this.getClass().getSimpleName() + "." + field.getName() + "  are public.", ex);
            }
            if (fieldOfThisInstance.equals(qdt)) {
                return field;
            }
        }
        return fieldReqd;
    }

    /**
     *
     * Requires the field to be from this instance to work
     *
     * @param qdt
     */
    public void ignoreForeignKey(QueryableDatatype qdt) {
        Field fieldOfFK = getFieldOf(qdt);
        if (fieldOfFK == null) {
            throw new IncorrectDBRowInstanceSuppliedException();
        }
        ignoredRelationships.add(fieldOfFK);
    }

    public void useAllForeignKeys() {
        ignoredRelationships.clear();
    }

    public void ignoreAllForeignKeys() {
        ignoredRelationships.addAll(this.getForeignKeyFields());
    }

    /**
     *
     * Creates a foreign key like relationship between columns on 2 different
     * DBRow objects
     *
     * this function relies on the QueryableDatatypes being part of the DBRows
     * that are also passed. So every call to this function should be similar
     * to:
     *
     * myRow.addRelationship(myRow.field1, myOtherRow, myOtherRow.field2);
     *
     * uses the default DBEqualsOperator
     *
     * @param thisTableField
     * @param otherTable
     * @param otherTableField
     */
    public void addRelationship(QueryableDatatype thisTableField, DBRow otherTable, QueryableDatatype otherTableField) {
        DBRelationship dbRelationship = new DBRelationship(this, thisTableField, otherTable, otherTableField);
        adHocRelationships.add(dbRelationship);
    }

    /**
     *
     * Creates a foreign key like relationship between columns on 2 different
     * DBRow objects
     *
     * this function relies on the QueryableDatatypes being part of the DBRows
     * that are also passed. So every call to this function should be similar
     * to:
     *
     * myRow.addRelationship(myRow.field1, myOtherRow, myOtherRow.field2);
     *
     * Uses the supplied operator to establish the relationship rather than the
     * default DBEqualsOperator. Not all operators can be used for
     * relationships.
     *
     * @param thisTableField
     * @param otherTable
     * @param otherTableField
     * @param operator
     */
    public void addRelationship(QueryableDatatype thisTableField, DBRow otherTable, QueryableDatatype otherTableField, DBOperator operator) {
        DBRelationship dbRelationship = new DBRelationship(this, thisTableField, otherTable, otherTableField, operator);
        adHocRelationships.add(dbRelationship);
    }

    /**
     *
     */
    public void clearRelationships() {
        this.adHocRelationships.clear();
    }

    List<String> getAdHocRelationshipSQL() {
        List<String> sqlStrings = new ArrayList<String>();
        for (DBRelationship rel : adHocRelationships) {
            sqlStrings.add(rel.generateSQL(database));
        }
        return sqlStrings;
    }

    protected boolean hasLargeObjectColumns() {
        if (hasBlobs == null) {
            hasBlobs = Boolean.FALSE;
            Map<String, QueryableDatatype> columnsAndQueryableDatatypes = getColumnsAndQueryableDatatypes();
            Collection<QueryableDatatype> values = columnsAndQueryableDatatypes.values();
            for (QueryableDatatype qdt : values) {
                if (qdt instanceof DBLargeObject) {
                    blobColumns.add((DBLargeObject) qdt);
                    hasBlobs = Boolean.TRUE;
                }
            }
        }
        return hasBlobs;
    }

    protected boolean hasSetLargeObjectColumns() {
        if (hasLargeObjectColumns()) {
            for (DBLargeObject qdt : blobColumns) {
                if (qdt instanceof QueryableDatatype && ((QueryableDatatype) qdt).hasChanged()) {
                    return true;
                }
            }
        }
        return false;
    }

    protected DBActionList<DBSaveBLOB> getLargeObjectActions() {
        DBActionList<DBSaveBLOB> actions = new DBActionList<DBSaveBLOB>();
        for (DBLargeObject blob : blobColumns) {
            actions.add(new DBSaveBLOB(this, blob));
        }
        return actions;
    }

    /**
     *
     * Requires the field to be from this instance to work
     *
     * @param qdt
     */
    public void returnFieldsLimitedTo(QueryableDatatype... qdts) {
        for (QueryableDatatype qdt : qdts) {
            Field fieldOfColumn = getFieldOf(qdt);
            if (fieldOfColumn == null) {
                throw new IncorrectDBRowInstanceSuppliedException();
            }
            returnColumns.add(fieldOfColumn);
        }
    }

    public void returnAllFields() {
        returnColumns.clear();
    }
}