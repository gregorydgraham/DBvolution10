package nz.co.gregs.dbvolution.internal;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import nz.co.gregs.dbvolution.annotations.DBTableName;
import nz.co.gregs.dbvolution.databases.definitions.DBDefinition;

/**
 * Wraps the class-type of an end-user's data model object.
 * Generally it's expected that the class is annotated with DBvolution annotations to mark
 * the table name and the fields or bean properties that map to columns, however
 * this class will work against any class type.
 * 
 * <p> Instances of this class should not be shared between different databases,
 * because it depends on the specific database's definition.
 * 
 * <p> Instances of this class are <i>thread-safe</i>.
 * @author Malcolm Lett
 */
public class ClassAdaptor {
	private final DBDefinition dbDefn;
	private final Class<?> adaptee;
	private final DBTableName tableNameAnnotation; // null if not on class
	private final String tableName; // null if missing @DBTableName
	
	/**
	 * All properties of which DBvolution is aware, ordered as first encountered.
	 * Requires that the {@code DBColumn} annotation is declared on the property.
	 */
	private final List<Property> properties;
	
	/**
	 * Column names normalized according to case sensitivity of database.
	 * If column names duplicated, stores only the first encountered of each column name.
	 * Assumes validation is done elsewhere in this class.
	 * Note: doesn't need to be synchronized because it's never modified once created.
	 */
	private final Map<String, Property> propertiesByNormalizedColumnName;

	/**
	 * Indexed by java property name.
	 */
	private final Map<String, Property> propertiesByPropertyName;
	
	public ClassAdaptor(DBDefinition dbDefn, Class<?> clazz) {
		this.dbDefn = dbDefn;
		this.adaptee = clazz;
		
		// pre-calculate table name annotation
		this.tableNameAnnotation = adaptee.getAnnotation(DBTableName.class);
		
		// pre-calculate table name
		// (null if no annotation)
		if (tableNameAnnotation != null) {
			String tableName = tableNameAnnotation.value();
			this.tableName = (tableName == null) ? adaptee.getSimpleName() : tableName;
		} else {
			this.tableName = null;
		}
		
		// pre-calculate properties list
		properties = new ArrayList<Property>();
		
		// pre-calculate properties index
		propertiesByNormalizedColumnName = new HashMap<String, Property>();
		propertiesByPropertyName = new HashMap<String, Property>();
		for (Property property: properties) {
			
		}
	}
	
	/**
	 * Checks all the annotations etc. and errors.
	 * @throws Exception if has any errors
	 */
	public void checkForErrors() throws Exception {
		// TODO: to be implemented
	}
	
	/**
	 * Gets the indicated table name.
	 * Applies defaulting if the {@link DBTableName} annotation is present but doesn't provide
	 * an explicit table name.
	 * 
	 * <p> If the {@link DBTableName} annotation is missing, this method returns {@code null}.
	 * 
	 * <p> Use {@link #getDBTableNameAnnotation} for low level access.
	 * @return the table name, if specified explicitly or implicitly.
	 */
	public String getTableName() {
		return tableName;
	}
	
	/**
	 * Gets the {@link DBTableName} annotation on the class, if it exists.
	 * @return the annotation or null
	 */
	public DBTableName getDBTableNameAnnotation() {
		return tableNameAnnotation;
	}
	
	/**
	 * Gets the property associated with the given column.
	 * If multiple properties are annotated for the same column, this method
	 * will return only the first.
	 * 
	 * <p> Only provides access to properties annotated with {@code DBColumn}.
	 * 
	 * <p> Assumes validation is applied elsewhere to prohibit duplication of 
	 * column names.
	 * @param columnName
	 * @return
	 */
	public Property getPropertyByColumn(String columnName) {
		return propertiesByNormalizedColumnName.get(normalizedColumnNameOf(columnName));
	}

	/**
	 * Gets the property by its java property name.
	 * <p> Only provides access to properties annotated with {@code DBColumn}.
	 * 
	 * @param propertyName
	 * @return
	 */
	public Property getPropertyByName(String propertyName) {
		return propertiesByPropertyName.get(propertyName);
	}
	
	/**
	 * Gets all properties annotated with {@code DBColumn}.
	 * @return
	 */
	public List<Property> getProperties() {
		return properties;
	}
	
	/**
	 * Normalises the case of column names for direct matching when looking
	 * up properties by column name.
	 * @param dbDefn
	 * @param columnName
	 * @return the case-normalised version of the column name
	 */
	protected String normalizedColumnNameOf(String columnName) {
		if (dbDefn.isColumnNamesCaseSensitive()) {
			return columnName;
		}
		else {
			return (columnName == null) ? null : columnName.toUpperCase();
		}
	}
	
}
