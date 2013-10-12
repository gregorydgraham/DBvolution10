package nz.co.gregs.dbvolution.internal;

import java.util.List;

import nz.co.gregs.dbvolution.DBPebkacException;
import nz.co.gregs.dbvolution.annotations.DBTableName;
import nz.co.gregs.dbvolution.databases.definitions.DBDefinition;

/**
 * tbd
 * 
 * <p> Instances of this class are lightweight and efficient to create,
 * and they are intended to be short lived.
 * Instances of this class must not be shared between different DBDatabase instances,
 * however they can be safely associated within a single DBDatabase instance.
 * 
 * <p> Instances of this class are <i>thread-safe</i>.
 * @author Malcolm Lett
 */
//TODO: consider whether this should be called ObjectWrapper to avoid overload of term "Adaptor"
public class ObjectAdaptor {
	private final DBDefinition dbDefn;
	private ClassAdaptor classAdaptor;
	private Object adaptee;
	
//	public ObjectAdaptor(DBDefinition dbDefn, Object obj) {
//		this.dbDefn = dbDefn;
//		this.adaptee = obj;
//		this.clazz = new ClassAdaptor(dbDefn, obj.getClass()); // TODO: lookup class cache for better performance
//	}

	ObjectAdaptor(DBDefinition dbDefn, ClassAdaptor classAdaptor, Object obj) {
		this.dbDefn = dbDefn;
		this.adaptee = obj;
		this.classAdaptor = classAdaptor;
	}

	/**
	 * Checks all the annotations etc. and errors.
	 * @throws Exception if has any errors
	 */
	public void checkForErrors() throws DBPebkacException {
		classAdaptor.checkForErrors();
	}
	
	/**
	 * Gets a string representation suitable for debugging.
	 */
	@Override
	public String toString() {
		if (isTable()) {
			return "ObjectAdapter<"+tableName()+":"+classAdaptor.adaptee().getName()+">";
		}
		else {
			return "ObjectAdapter<no-table:"+classAdaptor.adaptee().getName()+">";
		}
	}
	
	/**
	 * Gets the wrapped object type supported by this {@code ObjectAdaptor}.
	 * Note: this should be the same as the wrapped object's actual type.
	 * @return
	 */
	public Class<?> adapteeType() {
		return classAdaptor.adaptee();
	}
	
	/**
	 * Gets the object wrapped by this {@code ObjectAdaptor}.
	 * @return
	 */
	public Object adapteeObject() {
		return adaptee;
	}
	
	/**
	 * Indicates whether this class maps to a database column.
	 * @return
	 */
	public boolean isTable() {
		return classAdaptor.isTable();
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
	public String tableName() {
		return classAdaptor.tableName();
	}
	
	/**
	 * Gets the {@link DBTableName} annotation on the class, if it exists.
	 * @return the annotation or null
	 */
	public DBTableName getDBTableNameAnnotation() {
		return classAdaptor.getDBTableNameAnnotation();
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
	 * @param dbDefn active database definition
	 * @param columnName
	 * @return
	 */
	public DBProperty getPropertyByColumn(String columnName) {
		return classAdaptor.getPropertyByColumn(dbDefn, columnName);
	}

	/**
	 * Gets the property by its java property name.
	 * <p> Only provides access to properties annotated with {@code DBColumn}.
	 * 
	 * @param propertyName
	 * @return
	 */
	public DBProperty getPropertyByName(String propertyName) {
		return classAdaptor.getPropertyByName(propertyName);
	}
	
	/**
	 * Gets all properties annotated with {@code DBColumn}.
	 * @return
	 */
	public List<DBProperty> getProperties() {
		return classAdaptor.getProperties();
	}
}
