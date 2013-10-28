package nz.co.gregs.dbvolution;

import java.io.Serializable;
import java.lang.reflect.InvocationTargetException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import nz.co.gregs.dbvolution.actions.DBActionList;
import nz.co.gregs.dbvolution.actions.DBSaveBLOB;
import nz.co.gregs.dbvolution.databases.definitions.DBDefinition;
import nz.co.gregs.dbvolution.datatypes.DBLargeObject;
import nz.co.gregs.dbvolution.datatypes.QueryableDatatype;
import nz.co.gregs.dbvolution.exceptions.IncorrectDBRowInstanceSuppliedException;
import nz.co.gregs.dbvolution.internal.DBRowInstanceWrapper;
import nz.co.gregs.dbvolution.internal.DBRowWrapperFactory;
import nz.co.gregs.dbvolution.internal.PropertyWrapper;
import nz.co.gregs.dbvolution.internal.PropertyWrapperDefinition;
import nz.co.gregs.dbvolution.operators.DBOperator;

import org.reflections.Reflections;

/**
 *
 * @author gregory.graham
 */
abstract public class DBRow implements Serializable {

    private boolean isDefined = false;
    private final List<PropertyWrapperDefinition> ignoredForeignKeys = new ArrayList<PropertyWrapperDefinition>();
    private final List<PropertyWrapperDefinition> returnColumns = new ArrayList<PropertyWrapperDefinition>();
    private final List<PropertyWrapper> fkFields = new ArrayList<PropertyWrapper>();
    private final List<DBRelationship> adHocRelationships = new ArrayList<DBRelationship>();
    private HashMap<String, QueryableDatatype> columnsAndQDTs;
    private Boolean hasBlobs;
    private final List<PropertyWrapper> blobColumns = new ArrayList<PropertyWrapper>();
    static DBRowWrapperFactory wrapperFactory = new DBRowWrapperFactory();
    transient DBRowInstanceWrapper wrapper = null;
    private ArrayList<Class<? extends DBRow>> referencedTables;

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
    

    /**
     * 
     * @return non-null list of property wrappers, empty if none
     */
    protected List<PropertyWrapper> getPropertyWrappers() {
        return getWrapper().getPropertyWrappers();
    }

    public void clear() {
        for (PropertyWrapper prop : getPropertyWrappers()) {
        	QueryableDatatype qdt = prop.getQueryableDatatype();
        	if (qdt != null) {
        		qdt.clear();
        	}
        }
    }

    public QueryableDatatype getPrimaryKey() {
        final PropertyWrapper primaryKeyPropertyWrapper = getPrimaryKeyPropertyWrapper();
        if (primaryKeyPropertyWrapper == null) {
            return null;
        } else {
            QueryableDatatype queryableValueOfField = primaryKeyPropertyWrapper.getQueryableDatatype();
            return queryableValueOfField;
        }
    }

    /**
     *
     * indicates that the DBRow is defined in the database
     *
     * @param newValue
     */
    protected void setDefined(boolean newValue) {
        isDefined = newValue;
    }

    /**
     *
     * indicates if the DBRow is defined in the database
     *
     * @return true if the row is defined within the database, false otherwise
     */
    public boolean getDefined() {
        return isDefined;
    }

    public boolean hasChanged() {
        List<PropertyWrapper> propertyWrappers = getWrapper().getPropertyWrappers();
        for (PropertyWrapper prop : propertyWrappers) {
            if (prop.getQueryableDatatype().hasChanged()) {
                return true;
            }
        }
        return false;
    }

    @Deprecated
    public String getPrimaryKeySQLStringValue(DBDatabase db) {
        QueryableDatatype queryableValueOfField;
        final PropertyWrapper primaryKey = getPrimaryKeyPropertyWrapper();
        if (primaryKey != null) {
            queryableValueOfField = primaryKey.getQueryableDatatype();
            String pkColumnValue;
            if (queryableValueOfField.hasChanged()) {
                pkColumnValue = queryableValueOfField.getPreviousSQLValue(db);
            } else {
                pkColumnValue = queryableValueOfField.toSQLString(db);
            }
            return pkColumnValue;
        }
        return "";
    }

    public String getPrimaryKeyName() {
        PropertyWrapper primaryKeyPropertyWrapper = getPrimaryKeyPropertyWrapper();
        if (primaryKeyPropertyWrapper == null) {
            return null;
        } else {
            return primaryKeyPropertyWrapper.columnName();
        }

        //        final Field pkField = getPrimaryKeyField();
        //        if (pkField == null) {
        //            return null;
        //        } else {
        //            return pkField.getAnnotation(DBColumn.class).value();
        //        }
    }

    protected PropertyWrapper getPrimaryKeyPropertyWrapper() {
        return getWrapper().primaryKey();
//        
//            Class<? extends DBRow> thisClass = this.getClass();
//            Field[] fields = thisClass.getDeclaredFields();
//            for (Field field : fields) {
//                if (field.isAnnotationPresent(DBPrimaryKey.class)) {
//                    return field;
//                }
//            }
//            return null;
    }

//    /**
//     * DO NOT USE THIS.
//     *
//     * @param <Q>
//     * @param property
//     * @return
//     */
//    @SuppressWarnings("unchecked")
//    @Deprecated
//    public <Q extends QueryableDatatype> Q getQueryableValueOfPropertWrapper(PropertyWrapper property) {
//        return (Q) property.getQueryableDatatype();
//    }

//    @Deprecated
//    Map<String, QueryableDatatype> getColumnsAndQueryableDatatypes() {
//        if (columnsAndQDTs == null) {
//            columnsAndQDTs = new HashMap<String, QueryableDatatype>();
//            String columnName;
//            QueryableDatatype qdt;
//
//            List<PropertyWrapper> fields = getWrapper().getPropertyWrappers();
//            for (PropertyWrapper field : fields) {
//                if (field.isColumn()) {
//                    qdt = field.getQueryableDatatype();
//                    columnName = field.columnName();
//                    columnsAndQDTs.put(columnName, qdt);
//                }
//            }
//        }
//        return columnsAndQDTs;
//    }

    /**
     * @deprecated use of this method is likely a bug
     */
    @Deprecated
    public List<QueryableDatatype> getQueryableDatatypes() {
        List<PropertyWrapper> propertyWrappers = getWrapper().getPropertyWrappers();

        List<QueryableDatatype> arrayList = new ArrayList<QueryableDatatype>();
        for (PropertyWrapper prop : propertyWrappers) {
            arrayList.add(prop.getQueryableDatatype());
        }
//        arrayList.addAll(getColumnsAndQueryableDatatypes().values());
        return arrayList;
    }

    /**
     *
     * @return
     *
     */
    public String getWhereClause(DBDatabase db) {
//        this.setDatabase(db);
        DBDefinition defn = db.getDefinition();
        StringBuilder whereClause = new StringBuilder();
        List<PropertyWrapper> props = getWrapper().getPropertyWrappers();
        for (PropertyWrapper prop : props) {
            if (prop.isColumn()) {
                QueryableDatatype qdt = prop.getQueryableDatatype();
//                qdt.setDatabase(db);
                whereClause.append(qdt.getWhereClause(db, defn.formatTableAndColumnName(this.getTableName(), prop.columnName())));
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
    	return getWrapper().tableName();
    }

    @Override
    public String toString() {
        StringBuilder string = new StringBuilder();
        Class<? extends DBRow> thisClass = this.getClass();
        List<PropertyWrapper> fields = getWrapper().getPropertyWrappers();

        String separator = "";

        for (PropertyWrapper field : fields) {
            if (field.isColumn()) {
                string.append(separator);
                string.append(" ");
                string.append(field.javaName());
                string.append(":");
                string.append(field.getQueryableDatatype());
                separator = ",";
            }
        }
        return string.toString();
    }

    public String toStringMinusFKs() {
        StringBuilder string = new StringBuilder();
        Class<? extends DBRow> thisClass = this.getClass();
        List<PropertyWrapper> fields = getWrapper().getPropertyWrappers();

        String separator = "";

        for (PropertyWrapper field : fields) {
            if (field.isColumn()) {
                if (!field.isForeignKey()) {
                    string.append(separator);
                    string.append(" ");
                    string.append(field.javaName());
                    string.append(":");
                    string.append(field.getQueryableDatatype());
                    separator = ",";
                }
            }
        }
        return string.toString();
    }

    /**
     * @param database the database to set
     */
//    public void setDatabase(DBDatabase db) {
////        this.database = theDatabase;
//
//        for (Field field : this.getClass().getDeclaredFields()) {
//            if (field.isAnnotationPresent(DBColumn.class)) {
//                getQueryableValueOfField(field).setDatabase(database);
//            }
//        }
//    }
    public String getValuesClause(DBDatabase db) {
//        this.setDatabase(db);
        StringBuilder string = new StringBuilder();
        Class<? extends DBRow> thisClass = this.getClass();
        List<PropertyWrapper> props = getWrapper().getPropertyWrappers();

        String separator = " VALUES ( ";
        for (PropertyWrapper prop : props) {
            if (prop.isColumn()
                    && !DBLargeObject.class.isAssignableFrom(prop.type())) {
                final QueryableDatatype qdt = prop.getQueryableDatatype();
                string.append(separator).append(qdt.toSQLString(db));
                separator = ",";
            }
        }
        return string.append(")").toString();
    }

    protected String getSetClause(DBDatabase db) {
//        this.setDatabase(db);
        DBDefinition defn = db.getDefinition();
        StringBuilder sql = new StringBuilder();
        Class<? extends DBRow> thisClass = this.getClass();
        List<PropertyWrapper> fields = getWrapper().getPropertyWrappers();

        String separator = defn.getStartingSetSubClauseSeparator();
        for (PropertyWrapper field : fields) {
            if (field.isColumn()) {
                final QueryableDatatype qdt = field.getQueryableDatatype();
                if (qdt.hasChanged()) {
                    String columnName = field.columnName();
                    sql.append(separator)
                            .append(defn.formatColumnName(columnName))
                            .append(defn.getEqualsComparator())
                            .append(qdt
                                    .toSQLString(db));
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
//        Class<? extends DBRow> thisClass = this.getClass();
        List<PropertyWrapper> props = getWrapper().getPropertyWrappers();

        for (PropertyWrapper prop : props) {
            if (prop.isColumn()) {
                if (returnColumns == null || returnColumns.isEmpty() || returnColumns.contains(prop.getDefinition())) {
                    String dbColumnName = prop.columnName();
                    if (dbColumnName != null) {
                        columnNames.add(dbColumnName);
                    }
                }
            }
        }
        return columnNames;
    }

    /**
     * This method shouldn't be needed anymore because
     * PropertyWrappers or PropertyWrapperDefinitions should be passed around
     * instead of QDTs. In particular, with the introduction of
     * type adaptors, java fields aren't necessarily of type QDT.
     * @param qdt
     * @return
     * @deprecated use of this method indicates a likely bug
     */
    @Deprecated
    public String getDBColumnName(QueryableDatatype qdt) {
        return this.getPropertyWrapperOf(qdt).columnName();
    }

    @Deprecated
    protected static String getTableAndColumnName(DBDatabase db, DBRow[] baseRows, QueryableDatatype qdt) {
        String columnName;
        String tableName;
        String fullName = null;
        DBDefinition defn = db.getDefinition();
        for (DBRow row : baseRows) {
            tableName = row.getTableName();
            columnName = row.getDBColumnName(qdt);
            if (columnName != null) {
                fullName = defn.formatTableAndColumnName(tableName, columnName);
                return fullName;
            }
        }
        return fullName;
    }

//    protected Map<DBForeignKey, DBColumn> getForeignKeys() {
//        if (foreignKeys == null) {
//            foreignKeys = new HashMap<DBForeignKey, DBColumn>();
////        Class<? extends DBRow> thisClass = this.getClass();
//            List<PropertyWrapper> fields = this.getForeignKeyFields();
//
//            for (PropertyWrapper field : fields) {
//                if (!ignoredForeignKeys.contains(field)) {
//                    DBForeignKey annotation = field.getAnnotation(DBForeignKey.class);
//                    DBColumn columnName = field.getAnnotation(DBColumn.class);
//                    foreignKeys.put(annotation, columnName);
//                }
//            }
//        }
//        return foreignKeys;
//    }
    protected List<PropertyWrapper> getForeignKeyPropertyWrappers() {
        if (fkFields.isEmpty()) {
            List<PropertyWrapper> props = getWrapper().getPropertyWrappers();

            for (PropertyWrapper prop : props) {
                if (prop.isColumn()) {
                    if (prop.isForeignKey()) {
                        if (!ignoredForeignKeys.contains(prop.getDefinition())) {
                            fkFields.add(prop);
                        }
                    }
                }
            }
        }
        return fkFields;
    }
    
    /**
     * Gets a wrapper for the underlying property (field or method) given the
     * property's object reference.
     * 
     * <p> For example the following code snippet will get
     * a property wrapper for the {@code name} field:
     * <pre>
     * Customer customer = ...;
     * getPropertyWrapperOf(customer.name);
     * </pre>
     * @param qdt
     * @return
     */
    public PropertyWrapper getPropertyWrapperOf(Object qdt) {
        List<PropertyWrapper> props = getWrapper().getPropertyWrappers();

        Object qdtOfProp;
        for (PropertyWrapper prop : props) {
            qdtOfProp = prop.rawJavaValue();
            if (qdtOfProp == qdt) {
                return prop;
            }
        }
        return null;
    }

    /**
     * Ignores the foreign key of the property (field or method)
     * given the property's object reference.
     * 
     * <p> For example the following code snippet will ignore the
     * foreign key on the fkAddress field:
     * <pre>
     * Customer customer = ...;
     * customer.ignoreForeignKey(customer.fkAddress);
     * </pre>
     *
     * <p> Requires the field to be from this instance to work.
     * @param qdt
     */
    public void ignoreForeignKey(Object qdt) {
        PropertyWrapper fkProp = getPropertyWrapperOf(qdt);
        if (fkProp == null) {
            throw new IncorrectDBRowInstanceSuppliedException(this, qdt);
        }
        ignoredForeignKeys.add(fkProp.getDefinition());
        fkFields.clear();
    }

    public void useAllForeignKeys() {
        ignoredForeignKeys.clear();
        fkFields.clear();
    }

    public void ignoreAllForeignKeys() {
        List<PropertyWrapper> props = this.getForeignKeyPropertyWrappers();
        for (PropertyWrapper prop : props) {
            ignoredForeignKeys.add(prop.getDefinition());
        }
        fkFields.clear();
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

    List<String> getAdHocRelationshipSQL(DBDatabase db) {
        List<String> sqlStrings = new ArrayList<String>();
        DBDefinition defn = db.getDefinition();
        for (DBRelationship rel : adHocRelationships) {
            sqlStrings.add(defn.beginAndLine() + rel.generateSQL(db));
        }
        return sqlStrings;
    }

    protected boolean hasLargeObjectColumns() {
        if (hasBlobs == null) {
            hasBlobs = Boolean.FALSE;
            for (PropertyWrapper prop: getPropertyWrappers()) {
            	if (prop.isInstanceOf(DBLargeObject.class)) {
            		blobColumns.add(prop);
                    hasBlobs = Boolean.TRUE;
            	}
            }
            
//            Map<String, QueryableDatatype> columnsAndQueryableDatatypes = getColumnsAndQueryableDatatypes();
//            Collection<QueryableDatatype> values = columnsAndQueryableDatatypes.values();
//            for (QueryableDatatype qdt : values) {
//                if (qdt instanceof DBLargeObject) {
//                    blobColumns.add((DBLargeObject) qdt);
//                    hasBlobs = Boolean.TRUE;
//                }
//            }
        }
        return hasBlobs;
    }

    protected boolean hasSetLargeObjectColumns() {
        if (hasLargeObjectColumns()) {
            for (PropertyWrapper prop : blobColumns) {
            	QueryableDatatype qdt = prop.getQueryableDatatype();
            	if (qdt != null && qdt.hasChanged()) {
                    return true;
                }
            }
        }
        return false;
    }

    protected DBActionList getLargeObjectActions(DBDatabase db) {
        DBActionList actions = new DBActionList();
        for (PropertyWrapper prop : blobColumns) {
        	actions.add(new DBSaveBLOB(this, prop.columnName(),
        			(DBLargeObject)prop.getQueryableDatatype()));
        }
        return actions;
    }

    /**
     * Limits the returned columns by the specified properties (fields and/or methods)
     * given the properties' object references.
     * 
     * <p> For example the following code snippet will include
     * only the uid and name columns based on the uid and name fields:
     * <pre>
     * Customer customer = ...;
     * customer.returnFieldsLimitedTo(customer.uid, customer.name);
     * </pre>
     *
     * <p> Requires the field to be from this instance to work.
     *
     * @param qdt
     */
    public void returnFieldsLimitedTo(Object... qdts) {
        for (Object qdt : qdts) {
        	PropertyWrapper prop = getPropertyWrapperOf(qdt);
            if (prop == null) {
                throw new IncorrectDBRowInstanceSuppliedException(this, qdt);
            }
            returnColumns.add(prop.getDefinition());
        }
    }

    public void returnAllFields() {
        returnColumns.clear();
    }

    /**
     * @return the adHocRelationships
     */
    public List<DBRelationship> getAdHocRelationships() {
        return adHocRelationships;
    }

    /**
     * the foreign keys and adhoc relationships as an SQL String or a null
     * pointer
     *
     * @return the foreign keys and adhoc relationships as an SQL String or a
     * null pointer
     */
    public String getRelationshipsAsSQL(DBDatabase db, DBRow newTable) {
        StringBuilder rels = new StringBuilder();
        DBDefinition defn = db.getDefinition();
        final String lineSeparator = System.getProperty("line.separator");

        List<PropertyWrapper> fks = getForeignKeyPropertyWrappers();
        String joinSeparator = "";
        for (PropertyWrapper fk : fks) {
            Class<? extends DBRow> referencedClass = fk.referencedClass();
            PropertyWrapperDefinition referencedProp = fk.referencedPropertyDefinitionIdentity();

            if (newTable.getClass().equals(referencedClass)) {
                String formattedForeignKey = defn.formatTableAndColumnName(
                		fk.tableName(), fk.columnName());

                String formattedReferencedColumn = defn.formatTableAndColumnName(
                		referencedProp.tableName(), referencedProp.columnName());
                
                rels.append(lineSeparator)
                        .append(joinSeparator)
                        .append(formattedForeignKey)
                        .append(defn.getEqualsComparator())
                        .append(formattedReferencedColumn);

                joinSeparator = defn.beginAndLine();
            }
        }
        List<DBRelationship> adHocs = getAdHocRelationships();
        for (DBRelationship adhoc : adHocs) {
            DBRow firstTable = adhoc.getFirstTable();
            DBRow secondTable = adhoc.getSecondTable();
            DBRow leftTable = firstTable;
            DBRow rightTable = secondTable;
            PropertyWrapper leftColumn = adhoc.getFirstColumnPropertyWrapper();
            PropertyWrapper rightColumn = adhoc.getSecondColumnPropertyWrapper();
            DBOperator operator = adhoc.getOperation();

            if (rightTable.getClass().equals(this.getClass())) {
                leftTable = secondTable;
                rightTable = firstTable;
                leftColumn = adhoc.getSecondColumnPropertyWrapper();
                rightColumn = adhoc.getFirstColumnPropertyWrapper();
                operator = operator.getInverseOperator();
            }

            rels.append(lineSeparator)
                    .append(joinSeparator)
                    .append(DBRelationship.generateSQL(db, leftTable, leftColumn, operator, rightTable, rightColumn));

            joinSeparator = defn.beginAndLine();
        }

        adHocs = newTable.getAdHocRelationships();
        for (DBRelationship adhoc : adHocs) {
            DBRow firstTable = adhoc.getFirstTable();
            DBRow secondTable = adhoc.getSecondTable();
            DBRow leftTable = firstTable;
            DBRow rightTable = secondTable;
            PropertyWrapper leftColumn = adhoc.getFirstColumnPropertyWrapper();
            PropertyWrapper rightColumn = adhoc.getSecondColumnPropertyWrapper();
            DBOperator operator = adhoc.getOperation();

            if (rightTable.getClass().equals(this.getClass())) {
                leftTable = secondTable;
                rightTable = firstTable;
                leftColumn = adhoc.getSecondColumnPropertyWrapper();
                rightColumn = adhoc.getFirstColumnPropertyWrapper();
                operator = operator.getInverseOperator();
            }

            rels.append(lineSeparator)
                    .append(joinSeparator)
                    .append(DBRelationship.generateSQL(db, leftTable, leftColumn, operator, rightTable, rightColumn));

            joinSeparator = defn.beginAndLine();
        }

        fks = newTable.getForeignKeyPropertyWrappers();
        for (PropertyWrapper fk : fks) {
            Class<? extends DBRow> value = fk.referencedClass();

            if (this.getClass().equals(value)) {

                String fkColumnName = fk.columnName();
                String formattedForeignKey = defn.formatTableAndColumnName(
                        newTable.getTableName(),
                        fkColumnName);

                String formattedPrimaryKey = defn.formatTableAndColumnName(
                        this.getTableName(),
                        this.getPrimaryKeyName());

                rels.append(lineSeparator)
                        .append(joinSeparator)
                        .append(formattedPrimaryKey)
                        .append(defn.getEqualsComparator())
                        .append(formattedForeignKey);

                joinSeparator = defn.beginAndLine();
            }
        }
        return rels.toString();
    }

    /**
     * Returns all the DBRow subclasses referenced by foreign keys
     *
     * @return A list of DBRow subclasses referenced with
     * @DBForeignKey
     *
     */
    @SuppressWarnings("unchecked")
    public List<Class<? extends DBRow>> getReferencedTables() {
        if (referencedTables == null) {
            referencedTables = new ArrayList<Class<? extends DBRow>>();
            List<PropertyWrapper> props = getWrapper().getForeignKeyPropertyWrappers();
            for (PropertyWrapper prop : props) {
                referencedTables.add(prop.referencedClass());
            }
        }
        return (List<Class<? extends DBRow>>) referencedTables.clone();
    }

    public List<Class<? extends DBRow>> getAllRelatedTables() {
        List<Class<? extends DBRow>> relatedTables = getReferencedTables();
        Reflections reflections = new Reflections(this.getClass().getPackage().getName());

        Set<Class<? extends DBRow>> subTypes = reflections.getSubTypesOf(DBRow.class);
        for (Class<? extends DBRow> tableClass : subTypes) {
            DBRow newInstance;
            try {
                newInstance = tableClass.newInstance();
                if (newInstance.getReferencedTables().contains(this.getClass())) {
                    relatedTables.add(tableClass);
                }
            } catch (InstantiationException ex) {
                throw new RuntimeException(ex);
            } catch (IllegalAccessException ex) {
                throw new RuntimeException(ex);
            }
        }
        return relatedTables;
    }

    private DBRowInstanceWrapper getWrapper() {
        if (wrapper == null) {
            wrapper = wrapperFactory.instanceWrapperFor(this);
        }
        return wrapper;
    }

    void setUnchanged() {
        List<PropertyWrapper> propertyWrappers = getWrapper().getPropertyWrappers();
        for (PropertyWrapper prop : propertyWrappers) {
            prop.getQueryableDatatype().setUnchanged();
        }
    }
}
