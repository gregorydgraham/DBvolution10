package nz.co.gregs.dbvolution;

import java.io.Serializable;
import java.lang.reflect.InvocationTargetException;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Set;

import nz.co.gregs.dbvolution.query.DBRelationship;
import nz.co.gregs.dbvolution.annotations.DBPrimaryKey;
import nz.co.gregs.dbvolution.columns.BooleanColumn;
import nz.co.gregs.dbvolution.databases.definitions.DBDefinition;
import nz.co.gregs.dbvolution.datatypes.*;
import nz.co.gregs.dbvolution.exceptions.IncorrectDBRowInstanceSuppliedException;
import nz.co.gregs.dbvolution.columns.*;
import nz.co.gregs.dbvolution.expressions.DBExpression;
import nz.co.gregs.dbvolution.internal.properties.*;
import nz.co.gregs.dbvolution.internal.query.QueryOptions;
import nz.co.gregs.dbvolution.operators.DBOperator;

import org.reflections.Reflections;

/**
 *
 * @author gregory.graham
 */
abstract public class DBRow implements Serializable {

    static DBRowWrapperFactory wrapperFactory = new DBRowWrapperFactory();
    boolean isDefined = false;
    final List<PropertyWrapperDefinition> ignoredForeignKeys = new ArrayList<PropertyWrapperDefinition>();
    final List<PropertyWrapperDefinition> returnColumns = new ArrayList<PropertyWrapperDefinition>();
    final List<DBRelationship> adHocRelationships = new ArrayList<DBRelationship>();
    private transient Boolean hasBlobs;
    private transient final List<PropertyWrapper> fkFields = new ArrayList<PropertyWrapper>();
    private transient final List<PropertyWrapper> blobColumns = new ArrayList<PropertyWrapper>();
    private transient DBRowInstanceWrapper wrapper = null;
    private transient ArrayList<Class<? extends DBRow>> referencedTables;
    private String tableAlias;
    private Boolean emptyRow = true;

    /**
     * Creates a blank DBRow
     *
     */
    public DBRow() {
    }

    /**
     * Creates a new blank DBRow of the supplied subclass.
     *
     * @param <T>
     * @param requiredDBRowClass
     * @return a new blank version of the specified class
     */
    public static <T extends DBRow> T getDBRow(Class<T> requiredDBRowClass) {
        try {
            return requiredDBRowClass.getConstructor().newInstance();
        } catch (NoSuchMethodException ex) {
            throw new RuntimeException("Unable To Create " + requiredDBRowClass.getClass().getSimpleName() + ": Please ensure that the constructor of  " + requiredDBRowClass.getClass().getSimpleName() + " has no arguments, throws no exceptions, and is public. If you are using an Inner Class, make sure the inner class is \"static\" as well.", ex);
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
     * Creates a new instance of the supplied DBRow subclass and duplicates it's
     * values.
     *
     * @param <T>
     * @param originalRow
     * @return a new version of the specified row with values that duplicate the
     * original.
     */
    public static <T extends DBRow> T copyDBRow(T originalRow) {
        @SuppressWarnings("unchecked")
        T newRow = (T) DBRow.getDBRow(originalRow.getClass());
        newRow.isDefined = originalRow.isDefined;
        for (PropertyWrapperDefinition defn : originalRow.ignoredForeignKeys) {
            newRow.ignoredForeignKeys.add(defn);
        }
        for (PropertyWrapperDefinition defn : originalRow.returnColumns) {
            newRow.returnColumns.add(defn);
        }
        for (DBRelationship adhoc : originalRow.adHocRelationships) {
            newRow.adHocRelationships.add(adhoc);
        }

        List<PropertyWrapper> subclassFields = originalRow.getPropertyWrappers();
        for (PropertyWrapper field : subclassFields) {
            try {
                Object originalValue = field.rawJavaValue();
                if (originalValue instanceof QueryableDatatype) {
                    QueryableDatatype originalQDT = (QueryableDatatype) originalValue;
                    field.getDefinition().setRawJavaValue(newRow, originalQDT.copy());
                } else {
                    field.getDefinition().setRawJavaValue(newRow, originalValue);
                }
            } catch (IllegalArgumentException ex) {
                throw new RuntimeException(ex);
            }
        }
        return newRow;
    }

    /**
     * Creates a new LargeObjectColumn instance to help create
     * {@link DBExpression expressions}
     *
     * <p>
     * This method is the easy way to create a reference to the database column
     * represented by the field for use in creating complex expressions within
     * your query.
     *
     * <p>
     * For use with the
     * {@link DBQuery#addCondition(nz.co.gregs.dbvolution.expressions.BooleanExpression) DBQuery addCondition method}
     *
     * @param fieldOfThisInstance
     * @return A LargeObjectColumn representing the supplied field
     */
    public LargeObjectColumn column(DBLargeObject fieldOfThisInstance) {
        return new LargeObjectColumn(this, fieldOfThisInstance);
    }

    /**
     * Creates a new BooleanColumn instance to help create
     * {@link DBExpression expressions}
     *
     * <p>
     * This method is the easy way to create a reference to the database column
     * represented by the field for use in creating complex expressions within
     * your query.
     *
     * <p>
     * For use with the
     * {@link DBQuery#addCondition(nz.co.gregs.dbvolution.expressions.BooleanExpression) DBQuery addCondition method}
     *
     * @param fieldOfThisInstance
     * @return A LargeObjectColumn representing the supplied field
     */
    public BooleanColumn column(DBBoolean fieldOfThisInstance) {
        return new BooleanColumn(this, fieldOfThisInstance);
    }

    /**
     * Creates a new BooleanColumn instance to help create
     * {@link DBExpression expressions}
     *
     * <p>
     * This method is the easy way to create a reference to the database column
     * represented by the field for use in creating complex expressions within
     * your query.
     *
     * <p>
     * For use with the
     * {@link DBQuery#addCondition(nz.co.gregs.dbvolution.expressions.BooleanExpression) DBQuery addCondition method}
     *
     * @param fieldOfThisInstance
     * @return A Column representing the supplied field
     */
    public BooleanColumn column(Boolean fieldOfThisInstance) {
        return new BooleanColumn(this, fieldOfThisInstance);
    }

    /**
     * Creates a new StringColumn instance to help create
     * {@link DBExpression expressions}
     *
     * <p>
     * This method is the easy way to create a reference to the database column
     * represented by the field for use in creating complex expressions within
     * your query.
     *
     * <p>
     * For use with the
     * {@link DBQuery#addCondition(nz.co.gregs.dbvolution.expressions.BooleanExpression) DBQuery addCondition method}
     *
     * @param fieldOfThisInstance
     * @return A Column representing the supplied field
     */
    public StringColumn column(DBString fieldOfThisInstance) {
        return new StringColumn(this, fieldOfThisInstance);
    }

    /**
     * Creates a new StringColumn instance to help create
     * {@link DBExpression expressions}
     *
     * <p>
     * This method is the easy way to create a reference to the database column
     * represented by the field for use in creating complex expressions within
     * your query.
     *
     * <p>
     * For use with the
     * {@link DBQuery#addCondition(nz.co.gregs.dbvolution.expressions.BooleanExpression) DBQuery addCondition method}
     *
     * @param fieldOfThisInstance
     * @return A Column representing the supplied field
     */
    public StringColumn column(String fieldOfThisInstance) {
        return new StringColumn(this, fieldOfThisInstance);
    }

    /**
     * Creates a new NumberColumn instance to help create
     * {@link DBExpression expressions}
     *
     * <p>
     * This method is the easy way to create a reference to the database column
     * represented by the field for use in creating complex expressions within
     * your query.
     *
     * <p>
     * For use with the
     * {@link DBQuery#addCondition(nz.co.gregs.dbvolution.expressions.BooleanExpression) DBQuery addCondition method}
     *
     * @param fieldOfThisInstance
     * @return A Column representing the supplied field
     */
    public NumberColumn column(DBNumber fieldOfThisInstance) {
        return new NumberColumn(this, fieldOfThisInstance);
    }

    /**
     * Creates a new NumberColumn instance to help create
     * {@link DBExpression expressions}
     *
     * <p>
     * This method is the easy way to create a reference to the database column
     * represented by the field for use in creating complex expressions within
     * your query.
     *
     * <p>
     * For use with the
     * {@link DBQuery#addCondition(nz.co.gregs.dbvolution.expressions.BooleanExpression) DBQuery addCondition method}
     *
     * @param fieldOfThisInstance
     * @return A Column representing the supplied field
     */
    public NumberColumn column(Number fieldOfThisInstance) {
        return new NumberColumn(this, fieldOfThisInstance);
    }

    /**
     * Creates a new DateColumn instance to help create
     * {@link DBExpression expressions}
     *
     * <p>
     * This method is the easy way to create a reference to the database column
     * represented by the field for use in creating complex expressions within
     * your query.
     *
     * <p>
     * For use with the
     * {@link DBQuery#addCondition(nz.co.gregs.dbvolution.expressions.BooleanExpression) DBQuery addCondition method}
     *
     * @param fieldOfThisInstance
     * @return A Column representing the supplied field
     */
    public DateColumn column(DBDate fieldOfThisInstance) {
        return new DateColumn(this, fieldOfThisInstance);
    }

    /**
     * Creates a new DateColumn instance to help create
     * {@link DBExpression expressions}
     *
     * <p>
     * This method is the easy way to create a reference to the database column
     * represented by the field for use in creating complex expressions within
     * your query.
     *
     * <p>
     * For use with the
     * {@link DBQuery#addCondition(nz.co.gregs.dbvolution.expressions.BooleanExpression) DBQuery addCondition method}
     *
     * @param fieldOfThisInstance
     * @return A Column representing the supplied field
     */
    public DateColumn column(Date fieldOfThisInstance) {
        return new DateColumn(this, fieldOfThisInstance);
    }

    /**
     * Returns the PropertyWrappers used internally to maintain the relationship
     * between fields and columns
     *
     * @return non-null list of property wrappers, empty if none
     */
    public List<PropertyWrapper> getPropertyWrappers() {
        return getWrapper().getPropertyWrappers();
    }

    /**
     * Remove all the settings on all the fields of this DBRow
     */
    public void clear() {
        for (PropertyWrapper prop : getPropertyWrappers()) {
            QueryableDatatype qdt = prop.getQueryableDatatype();
            if (qdt != null) {
                qdt.clear();

                // ensure field set when using type adaptors
                prop.setQueryableDatatype(qdt);
            }
        }
    }

    /**
     * Returns the QueryableDatatype instance of the Primary Key of This DBRow
     *
     * <p>
     * If the DBRow class has a {@link DBPrimaryKey @DBPrimaryKey} designated
     * field, then the QueryableDatatype instance of that field is returned.
     *
     * @return the QDT of the primary key or null if there is no primary key.
     */
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
     * Indicates that the DBRow is a defined row within the database.
     *
     * Example objects and blank rows from an optional table are "undefined".
     *
     * @param newValue TRUE if this row exists within the database.
     */
    @Deprecated
    private void setDefined(boolean newValue) {
        isDefined = newValue;
    }

    /**
     *
     * Indicates whether this instance is a defined row within the database.
     *
     * Example objects and blank rows from an optional table are "undefined".
     *
     * @return TRUE if this row exists within the database, otherwise FALSE.
     */
    public boolean getDefined() {
        return isDefined;
    }

    /**
     *
     * indicates the DBRow is not defined in the database
     *
     */
    public void setUndefined() {
        isDefined = false;
    }

    /**
     *
     * indicates the DBRow is defined in the database
     *
     */
    public void setDefined() {
        isDefined = true;
    }

    /**
     * Returns true if any of the non-LargeObject fields has been changed.
     *
     * @return TRUE if the simple types have changed, otherwise FALSE
     */
    public boolean hasChangedSimpleTypes() {
        List<PropertyWrapper> propertyWrappers = getWrapper().getPropertyWrappers();
        for (PropertyWrapper prop : propertyWrappers) {
            if (!(prop.getQueryableDatatype() instanceof DBLargeObject)) {
                if (prop.getQueryableDatatype().hasChanged()) {
                    return true;
                }
            }
        }
        return false;
    }

    public boolean setSimpleTypesToUnchanged() {
        List<PropertyWrapper> propertyWrappers = getWrapper().getPropertyWrappers();
        for (PropertyWrapper prop : propertyWrappers) {
            final QueryableDatatype qdt = prop.getQueryableDatatype();
            if (!(qdt instanceof DBLargeObject)) {
                if (qdt.hasChanged()) {
                    qdt.setUnchanged();
                }
            }
        }
        return false;
    }

    /**
     * Finds the Primary Key, if there is one, and returns its column name
     *
     * @return the column of the primary key
     */
    public String getPrimaryKeyColumnName() {
        PropertyWrapper primaryKeyPropertyWrapper = getPrimaryKeyPropertyWrapper();
        if (primaryKeyPropertyWrapper == null) {
            return null;
        } else {
            return primaryKeyPropertyWrapper.columnName();
        }
    }

    protected PropertyWrapper getPrimaryKeyPropertyWrapper() {
        return getWrapper().primaryKey();
    }

    /**
     * Change all the criteria specified on this DBRow instance into a list of
     * strings for adding in to the WHERE clause
     *
     * @param db The DBDatabase instance that this query is to be executed on.
     * @return the WHERE clause that will be used with the current parameters
     *
     */
    public List<String> getWhereClauses(DBDatabase db) {
        return getWhereClauses(db, false);
    }

    List<String> getWhereClauses(DBDatabase db, boolean useTableAlias) {
        DBDefinition defn = db.getDefinition();
        List<String> whereClause = new ArrayList<String>();
        List<PropertyWrapper> props = getWrapper().getPropertyWrappers();
        for (PropertyWrapper prop : props) {
            if (prop.isColumn()) {
                QueryableDatatype qdt = prop.getQueryableDatatype();
                String possibleWhereClause;
                if (useTableAlias) {
                    possibleWhereClause = qdt.getWhereClause(db, defn.formatTableAliasAndColumnName(this, prop.columnName()));
                } else {
                    possibleWhereClause = qdt.getWhereClause(db, defn.formatTableAndColumnName(this, prop.columnName()));
                }
                if (!possibleWhereClause.replaceAll(" ", "").isEmpty()) {
                    whereClause.add("(" + possibleWhereClause + ")");
                }
            }
        }
        return whereClause;
    }

    /**
     * Tests whether this DBRow instance has any criteria
     * ({@link QueryableDatatype#permittedValues(java.lang.Object[])}, etc) set.
     *
     * <p>
     * The database is not accessed and this method does not protect against
     * functionally blank queries.
     *
     * @param db
     * @return true if this DBRow instance has no specified criteria and will
     * create a blank query returning the whole table.
     *
     */
    public boolean willCreateBlankQuery(DBDatabase db) {
        List<String> whereClause = getWhereClauses(db);
        return whereClause == null || whereClause.isEmpty();
    }

    /**
     * Probably not needed by the programmer, this is the convenience function
     * to find the table name specified by {@code @DBTableName} or the class
     * name
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

    /**
     * Returns the same result as {@link #toString() } but omitting the Foreign
     * Key references.
     *
     * @return a string representation of the contents of this instance with
     * Foreign Key fields removed
     */
    public String toStringMinusFKs() {
        StringBuilder string = new StringBuilder();
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
     * Returns a list of the column names used by the database.
     *
     * @return A list of all raw, unformatted column names
     */
    protected List<String> getColumnNames() {
        ArrayList<String> columnNames = new ArrayList<String>();
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
     *
     * @return a list of all foreign keys, MINUS the ignored foreign keys
     */
    protected List<PropertyWrapper> getForeignKeyPropertyWrappers() {
        if (fkFields.isEmpty()) {
            List<PropertyWrapper> props = getWrapper().getForeignKeyPropertyWrappers();

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
     * <p>
     * For example the following code snippet will get a property wrapper for
     * the {@literal name} field:
     * <pre>
     * Customer customer = ...;
     * getPropertyWrapperOf(customer.name);
     * </pre>
     *
     * @param qdt
     * @return the PropertyWrapper associated with the Object suppled.
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
     * Ignores the foreign key of the property (field or method) given the
     * property's object reference.
     *
     * <p>
     * For example the following code snippet will ignore the foreign key on the
     * fkAddress field:
     * <pre>
     * Customer customer = ...;
     * customer.ignoreForeignKey(customer.fkAddress);
     * </pre>
     *
     * <p>
     * Requires the field to be from this instance to work.
     *
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

    /**
     * Removes all foreign keys from the "ignore" list.
     *
     */
    public void useAllForeignKeys() {
        ignoredForeignKeys.clear();
        fkFields.clear();
    }

    /**
     * Adds All foreign keys to the ignore list.
     *
     */
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
     * DBRow objects.
     *
     * <p>
     * This function relies on the QueryableDatatypes being part of the DBRows
     * that are also passed. So every call to this function should be similar
     * to:
     *
     * <p>
     * myRow.addRelationship(myRow.someField, myOtherRow,
     * myOtherRow.otherField);
     *
     * <p>
     * Uses the default DBEqualsOperator.
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
     * DBRow objects.
     *
     * <p>
     * this function relies on the QueryableDatatypes being part of the DBRows
     * that are also passed. So every call to this function should be similar
     * to:
     *
     * <p>
     * myRow.addRelationship(myRow.someField, myOtherRow, myOtherRow.otherField,
     * new DBGreaterThanOperator());
     *
     * <p>
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
     * Remove all added relationships.
     *
     *
     * @see
     * DBRow#addRelationship(nz.co.gregs.dbvolution.datatypes.QueryableDatatype,
     * nz.co.gregs.dbvolution.DBRow,
     * nz.co.gregs.dbvolution.datatypes.QueryableDatatype)
     * @see
     * DBRow#addRelationship(nz.co.gregs.dbvolution.datatypes.QueryableDatatype,
     * nz.co.gregs.dbvolution.DBRow,
     * nz.co.gregs.dbvolution.datatypes.QueryableDatatype,
     * nz.co.gregs.dbvolution.operators.DBOperator)
     */
    public void clearRelationships() {
        this.adHocRelationships.clear();
    }

    protected boolean hasLargeObjects() {
        if (hasBlobs == null) {
            hasBlobs = Boolean.FALSE;
            for (PropertyWrapper prop : getPropertyWrappers()) {
                if (prop.isInstanceOf(DBLargeObject.class)) {
                    blobColumns.add(prop);
                    hasBlobs = Boolean.TRUE;
                }
            }
        }
        return hasBlobs;
    }

    public boolean hasChangedLargeObjects() {
        if (hasLargeObjects()) {
            for (PropertyWrapper prop : blobColumns) {
                QueryableDatatype qdt = prop.getQueryableDatatype();
                if (qdt != null && qdt.hasChanged()) {
                    return true;
                }
            }
        }
        return false;
    }

    public List<PropertyWrapper> getChangedLargeObjects() {
        List<PropertyWrapper> changed = new ArrayList<PropertyWrapper>();
        if (hasLargeObjects()) {
            for (PropertyWrapper prop : blobColumns) {
                QueryableDatatype qdt = prop.getQueryableDatatype();
                if (qdt instanceof DBLargeObject) {
                    DBLargeObject large = (DBLargeObject) qdt;
                    if (large.hasChanged()) {
                        changed.add(prop);
                    }
                }
            }
        }
        return changed;
    }

    /**
     * Limits the returned columns by the specified properties (fields and/or
     * methods) given the properties object references.
     *
     * <p>
     * For example the following code snippet will include only the uid and name
     * columns based on the uid and name fields:
     * <pre>
     * Customer customer = ...;
     * customer.returnFieldsLimitedTo(customer.uid, customer.name);
     * </pre>
     *
     * <p>
     * Requires the field to be from this instance to work.
     *
     * @param <T> A list or List of fields of this DBRow
     * @param properties a list of fields/methods from this object
     */
    //@SafeVarargs
    public final <T> void returnFieldsLimitedTo(T... properties) {
        PropertyWrapper propWrapper;
        for (T property : properties) {
            propWrapper = getPropertyWrapperOf(property);
            if (propWrapper == null) {
                throw new IncorrectDBRowInstanceSuppliedException(this, property);
            }
            returnColumns.add(propWrapper.getDefinition());
        }
    }

    /**
     * Remove all limitations on the fields returned.
     *
     * <p>
     * Clears the limits on returned fields set by {@link DBRow#returnFieldsLimitedTo(T[])
     * }
     *
     *
     */
    public void returnAllFields() {
        returnColumns.clear();
    }

    /**
     * Needed for non-ANSI support.
     *
     * @return the adHocRelationships
     */
    protected List<DBRelationship> getAdHocRelationships() {
        return adHocRelationships;
    }

    /**
     * the foreign keys and adhoc relationships as an SQL String or a null
     * pointer
     *
     * @param db
     * @param newTable
     * @param options
     * @return the foreign keys and adhoc relationships as an SQL String or a
     * null pointer
     */
    protected String getRelationshipsAsSQL(DBDatabase db, DBRow newTable, QueryOptions options) {
        StringBuilder rels = new StringBuilder();
        DBDefinition defn = db.getDefinition();
//        final String lineSeparator = System.getProperty("line.separator");

        List<PropertyWrapper> fks = getForeignKeyPropertyWrappers();
        String joinSeparator = "";
        for (PropertyWrapper fk : fks) {
            Class<? extends DBRow> referencedClass = fk.referencedClass();

            if (referencedClass.isAssignableFrom(newTable.getClass())) {
                String formattedForeignKey = defn.formatTableAliasAndColumnName(
                        this, fk.columnName());

                String formattedReferencedColumn = defn.formatTableAliasAndColumnName(
                        newTable, fk.referencedColumnName());

                rels//.append(lineSeparator)
                        .append(joinSeparator)
                        .append(formattedForeignKey)
                        .append(defn.getEqualsComparator())
                        .append(formattedReferencedColumn);

                joinSeparator = defn.beginWhereClauseLine(options);
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

            rels//.append(lineSeparator)
                    .append(joinSeparator)
                    .append(DBRelationship.toSQLString(db, leftTable, leftColumn, operator, rightTable, rightColumn));

            joinSeparator = defn.beginWhereClauseLine(options);
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

            rels//.append(lineSeparator)
                    .append(joinSeparator)
                    .append(DBRelationship.toSQLString(db, leftTable, leftColumn, operator, rightTable, rightColumn));

            joinSeparator = defn.beginWhereClauseLine(options);
        }

        fks = newTable.getForeignKeyPropertyWrappers();
        for (PropertyWrapper fk : fks) {
            Class<? extends DBRow> value = fk.referencedClass();

            if (this.getClass().equals(value)) {

                String fkColumnName = fk.columnName();
                String formattedForeignKey = defn.formatTableAliasAndColumnName(
                        newTable,
                        fkColumnName);

                String formattedPrimaryKey = defn.formatTableAliasAndColumnName(
                        this,
                        this.getPrimaryKeyColumnName());

                rels//.append(lineSeparator)
                        .append(joinSeparator)
                        .append(formattedPrimaryKey)
                        .append(defn.getEqualsComparator())
                        .append(formattedForeignKey);

                joinSeparator = defn.beginWhereClauseLine(options);
            }
        }
        return rels.toString();
    }

    /**
     * Tests whether this instance of DBRow and the otherTable instance of DBRow
     * will be connected given the specified database and query options.
     *
     * @param database
     * @param otherTable
     * @param options
     * @return
     */
    public boolean willBeConnectedTo(DBDatabase database, DBRow otherTable, QueryOptions options) {
        String join = this.getRelationshipsAsSQL(database, otherTable, options);
        return join != null && !join.isEmpty();
    }

    /**
     * Returns all the DBRow subclasses referenced by by this class with foreign
     * keys
     *
     * <p>
     * Similar to {@link #getAllRelatedTables() } but where this class directly
     * references the external DBRow subclass with an {@code @DBForeignKey}
     * annotation.
     *
     * <p>
     * That is to say: where A is this class, returns a List of B such that A =>
     * B
     *
     * @return A list of DBRow subclasses referenced with {@code @DBForeignKey}
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

    /**
     * Creates a list of all DBRow subclasses that reference this class with
     * foreign keys.
     *
     * <p>
     * Similar to {@link #getReferencedTables() } but where this class is being
     * referenced by the external DBRow subclass.
     *
     * <p>
     * That is to say: where A is this class, returns a List of B such that B =>
     * A
     *
     * @return a list of classes that have a {@code @DBForeignKey} reference to
     * this class
     */
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

    public List<PropertyWrapper> getLargeObjects() {
        // Initialise the blob columns list if necessary
        if (hasLargeObjects()) {
            return blobColumns;
        } else {
            return blobColumns;
        }
    }

    /**
     * Finds all instances of {@code example} that share a {@link DBQueryRow}
     * with this instance.
     *
     * @param <R>
     * @param query
     * @param example
     * @return all instances of {@code example} that are connected to this
     * instance in the {@code query}
     * @throws SQLException
     */
    public <R extends DBRow> List<R> getRelatedInstancesFromQuery(DBQuery query, R example) throws SQLException {
        List<R> instances = new ArrayList<R>();
        for (DBQueryRow qrow : query.getAllRows()) {
            DBRow versionOfThis = qrow.get(this);
            R versionOfThat = qrow.get(example);
            if (versionOfThis.equals(this) && versionOfThat != null) {
                instances.add(versionOfThat);
            }
        }
        return instances;
    }

    protected void setTableAlias(String alias) {
        tableAlias = alias;
    }

    protected String getTableAlias() {
        return tableAlias == null ? getTableName() : tableAlias;
    }

    /**
     * Indicates whether this instance has any values set from the database.
     *
     * <p>
     * If this row is the result of the database sending back a row with NULL in
     * every column, this method returns TRUE.
     *
     * <p>
     * An empty row is probably the result an optional DBRow not having a
     * matching row for the query. In database parlance this row is a null row
     * of an OUTER JOIN and this table did not have any matching rows.
     *
     * <p>
     * Please note: if the row is undefined
     * {@link DBRow#isDefined (see isDefined)} then this is meaningless
     *
     * @return TRUE if the row has no non-null values or is undefined, FALSE
     * otherwise
     */
    public Boolean isEmptyRow() {
        return emptyRow;
    }

    /**
     * Sets the row to be empty or not.
     *
     * Used within DBQuery and DBTable while creating the DBRows to indicate an
     * empty row.
     *
     * @param isThisRowEmpty
     */
    protected void setEmptyRow(Boolean isThisRowEmpty) {
        this.emptyRow = isThisRowEmpty;
    }

}
