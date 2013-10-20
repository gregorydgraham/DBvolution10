package nz.co.gregs.dbvolution.internal;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import javax.naming.OperationNotSupportedException;

import nz.co.gregs.dbvolution.DBDatabase;
import nz.co.gregs.dbvolution.annotations.DBTableName;
import nz.co.gregs.dbvolution.databases.definitions.DBDefinition;
import nz.co.gregs.dbvolution.exceptions.DBPebkacException;
import nz.co.gregs.dbvolution.exceptions.DBRuntimeException;
import nz.co.gregs.dbvolution.internal.JavaPropertyFinder.PropertyType;
import nz.co.gregs.dbvolution.internal.JavaPropertyFinder.Visibility;

/**
 * Wraps the class-type of an end-user's data model object. Generally it's
 * expected that the class is annotated with DBvolution annotations to mark the
 * table name and the fields or bean properties that map to columns, however
 * this class will work against any class type.
 *
 * <p> To wrap a target object instance, use the
 * {@link #instanceAdaptorFor(DBDefinition, Object) objectAdapterFor()} method.
 *
 * <p> Note: instances of this class are expensive to create, and are intended
 * to be cached and kept long-term. Instances can be safely shared between
 * DBDatabase instances for different database types.
 *
 * <p> Instances of this class are <i>thread-safe</i>.
 *
 * @author Malcolm Lett
 */
public class DBRowClassWrapper {

    private final Class<?> adaptee;
    private final TableHandler tableHandler;
    /**
     * The properties that form the primary key, or empty if none.
     */
    private final List<PropertyWrapperDefinition> primaryKeyProperties;
    /**
     * All properties of which DBvolution is aware, ordered as first
     * encountered. Properties are only included if they are columns.
     */
    private final List<PropertyWrapperDefinition> properties;
    /**
     * Column names with original case for doing lookups on case-sensitive
     * databases. If column names duplicated, stores only the first encountered
     * of each column name. Assumes validation is done elsewhere in this class.
     * Note: doesn't need to be synchronized because it's never modified once
     * created.
     */
    private final Map<String, PropertyWrapperDefinition> propertiesByCaseSensitiveColumnName;
    /**
     * Column names normalized to upper case for doing lookups on
     * case-insensitive databases. If column names duplicated, stores only the
     * first encountered of each column name. Assumes validation is done
     * elsewhere in this class. Note: doesn't need to be synchronized because
     * it's never modified once created.
     */
    private final Map<String, PropertyWrapperDefinition> propertiesByUpperCaseColumnName;
    /**
     * Lists of properties that have duplicated columns if-and-only-if using a
     * case-insensitive database. We don't know in advance whether the database
     * in use is case-insensitive or not. So we give case-different duplicates
     * the benefit of doubt and just record until later. If this class is
     * accessed for use on a case-insensitive database the exception will be
     * thrown then, on first access to this class.
     */
    private final Map<String, List<PropertyWrapperDefinition>> duplicatedPropertiesByUpperCaseColumnName;
    /**
     * Indexed by java property name.
     */
    private final Map<String, PropertyWrapperDefinition> propertiesByPropertyName;

    public DBRowClassWrapper(Class<?> clazz) {
        this.adaptee = clazz;

        // annotation handlers
        this.tableHandler = new TableHandler(clazz);

        // pre-calculate properties list
        JavaPropertyFinder propertyFinder = new JavaPropertyFinder(
                Visibility.PRIVATE, Visibility.PUBLIC,
                JavaPropertyFilter.COLUMN_PROPERTY_FILTER,
                PropertyType.FIELD, PropertyType.BEAN_PROPERTY);

        properties = new ArrayList<PropertyWrapperDefinition>();
        for (JavaProperty javaProperty : propertyFinder.getPropertiesOf(clazz)) {
            PropertyWrapperDefinition property = new PropertyWrapperDefinition(javaProperty);
            if (property.isColumn()) {
                properties.add(property);
            }
        }

        // pre-calculate primary key
        List<PropertyWrapperDefinition> primaryKeyProperties = new ArrayList<PropertyWrapperDefinition>();
        for (PropertyWrapperDefinition property : properties) {
            if (property.isPrimaryKey()) {
                primaryKeyProperties.add(property);
            }
        }
        this.primaryKeyProperties = primaryKeyProperties;

        // pre-calculate properties index
        propertiesByCaseSensitiveColumnName = new HashMap<String, PropertyWrapperDefinition>();
        propertiesByUpperCaseColumnName = new HashMap<String, PropertyWrapperDefinition>();
        propertiesByPropertyName = new HashMap<String, PropertyWrapperDefinition>();
        duplicatedPropertiesByUpperCaseColumnName = new HashMap<String, List<PropertyWrapperDefinition>>();
        for (PropertyWrapperDefinition property : properties) {
            propertiesByPropertyName.put(property.javaName(), property);

            // add unique values for case-sensitive lookups
            // (error immediately on collisions)
            if (propertiesByCaseSensitiveColumnName.containsKey(property.columnName())) {
                throw new DBPebkacException("Class " + clazz.getName() + " has multiple properties for column " + property.columnName());
            } else {
                propertiesByCaseSensitiveColumnName.put(property.columnName(), property);
            }

            // add unique values for case-insensitive lookups
            // (defer erroring until actually know database is case insensitive)
            if (propertiesByUpperCaseColumnName.containsKey(property.columnName().toUpperCase())) {
                List<PropertyWrapperDefinition> list = duplicatedPropertiesByUpperCaseColumnName.get(property.columnName().toUpperCase());
                if (list == null) {
                    list = new ArrayList<PropertyWrapperDefinition>();
                    list.add(propertiesByUpperCaseColumnName.get(property.columnName().toUpperCase()));
                }
                list.add(property);
                duplicatedPropertiesByUpperCaseColumnName.put(property.columnName().toUpperCase(), list);
            } else {
                propertiesByUpperCaseColumnName.put(property.columnName().toUpperCase(), property);
            }
        }
    }

    /**
     * Checks for errors that can't be known in advance without knowing the
     * database being accessed.
     *
     * @param database active database
     */
    private void checkForRemainingErrorsOnAcccess(DBDatabase database) {
        // check for case-differing duplicate columns
        if (database.getDefinition().isColumnNamesCaseSensitive()) {
            if (!duplicatedPropertiesByUpperCaseColumnName.isEmpty()) {
                StringBuilder buf = new StringBuilder();
                for (List<PropertyWrapperDefinition> properties : duplicatedPropertiesByUpperCaseColumnName.values()) {
                    for (PropertyWrapperDefinition property : properties) {
                        if (buf.length() > 0) {
                            buf.append(", ");
                        }
                        buf.append(property.columnName());
                    }
                }

                throw new DBRuntimeException("The following columns are referenced multiple times on case-insensitive databases: " + buf.toString());
            }
        }
    }

    /**
     * Gets an object adaptor instance for the given target object on the given
     * active database definition.
     *
     * @param database active database
     * @param target
     * @return
     */
    public DBRowInstanceWrapper instanceAdaptorFor(Object target) {
//		checkForRemainingErrorsOnAcccess(database);
        return new DBRowInstanceWrapper(this, target);
    }

    /**
     * Gets a string representation suitable for debugging.
     */
    @Override
    public String toString() {
        if (isTable()) {
            return getClass().getSimpleName() + "<" + tableName() + ":" + adaptee.getName() + ">";
        } else {
            return getClass().getSimpleName() + "<no-table:" + adaptee.getName() + ">";
        }
    }

    /**
     * Gets the underlying wrapped class.
     *
     * @return
     */
    public Class<?> adaptee() {
        return adaptee;
    }

    /**
     * Gets the simple name of the class being wrapped by this adaptor.
     * <p> Use {@link #tableName()} for the name of the table mapped to this
     * class.
     *
     * @return
     */
    public String javaName() {
        return adaptee.getSimpleName();
    }

    /**
     * Gets the fully qualified name of the class being wrapped by this adaptor.
     * <p> Use {@link #tableName()} for the name of the table mapped to this
     * class.
     *
     * @return
     */
    public String qualifiedJavaName() {
        return adaptee.getName();
    }

    /**
     * Indicates whether this class maps to a database column.
     *
     * @return
     */
    public boolean isTable() {
        return tableHandler.isTable();
    }

    /**
     * Gets the indicated table name. Applies defaulting if the
     * {@link DBTableName} annotation is present but doesn't provide an explicit
     * table name.
     *
     * <p> If the {@link DBTableName} annotation is missing, this method returns
     * {@code null}.
     *
     * <p> Use {@link #getDBTableNameAnnotation} for low level access.
     *
     * @return the table name, if specified explicitly or implicitly.
     */
    public String tableName() {
        return tableHandler.getTableName();
    }

    /**
     * Gets the properties that together form the primary key, if any are
     * marked. In most tables this will be exactly one property.
     *
     * @return the non-empty list of properties, or null if no primary key
     */
    public PropertyWrapperDefinition primaryKey() {
        if (primaryKeyProperties.size() > 1) {
            throw new UnsupportedOperationException("Multi-Column Primary Keys are not yet supported.");
            //TODO
        } else {
            return primaryKeyProperties.get(0);
        }
    }

    /**
     * Gets the property associated with the given column. If multiple
     * properties are annotated for the same column, this method will return
     * only the first.
     *
     * <p> Only provides access to properties annotated with {@code DBColumn}.
     *
     * <p> Assumes validation is applied elsewhere to prohibit duplication of
     * column names.
     *
     * @param database active database
     * @param columnName
     * @return
     */
    public PropertyWrapperDefinition getPropertyByColumn(DBDatabase database, String columnName) {
        if (database.getDefinition().isColumnNamesCaseSensitive()) {
            return propertiesByUpperCaseColumnName.get(columnName.toUpperCase());
        } else {
            return propertiesByCaseSensitiveColumnName.get(columnName);
        }
    }

    /**
     * Gets the property by its java property name.
     * <p> Only provides access to properties annotated with {@code DBColumn}.
     *
     * <p> It's legal for a field and bean-property to have the same name, and
     * to both be annotated, but for different columns. This method doesn't
     * handle that well and returns only the first one it sees.
     *
     * @param propertyName
     * @return
     */
    public PropertyWrapperDefinition getPropertyByName(String propertyName) {
        return propertiesByPropertyName.get(propertyName);
    }

    /**
     * Gets all properties annotated with {@code DBColumn}.
     *
     * @return
     */
    public List<PropertyWrapperDefinition> getProperties() {
        return properties;
    }
// shouldn't be needed
//	/**
//	 * Gets the {@link DBTableName} annotation on the class, if it exists.
//	 * @return the annotation or null
//	 */
//	public DBTableName getDBTableNameAnnotation() {
//		return tableHandler.getDBTableNameAnnotation();
//	}
}
