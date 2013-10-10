package nz.co.gregs.dbvolution.internal;

/**
 * Adapts a single field or bean property within a class.
 * Includes support for understanding all DBv annotations.
 * 
 * <p> Provides a virtual view over the property that is internally
 * managed as a property's declared type and value which is
 * then mapped by the property's type adaptor.
 * If no explicit type adaptor is provided, then a dummy adaptor is used
 * which returns the same type and value as declared.
 * 
 * <p> 
 * The virtual view of the property is the type and value
 * returned by the adaptor, given the declared type and value on the
 * property itself - which is populated from the database.
 * 
 * <p> It is assumed that type adaptors allow mapping from
 * a DBv property type to a non-DBv type.
 * 
 * <p> Properties have the following types and values:
 * <ul>
 * <li> objectType/objectValue - the type and value actually stored on the declared property
 * <li> dbvType/dbvValue - the type and value used within DBv (a QueryableDataType)
 * <li> databaseType/databaseValue - the type and value of the database itself (this class doesn't deal with these) 
 * </ul>
 */
public interface Property {
	/**
	 * Gets the declared type of the property in the end-user's object.
	 * @return
	 */
	public Class<?> getObjectType();
	
	public Object getObjectValue();
	
	public void setObjectValue(Object value);
	
	/**
	 * Gets the mapped DBv type, possibly after type adaptor conversion.
	 * @return
	 */
	public Class<?> getDBvType();
	
	public Object getDBvValue();
	
	public void setDBvValue(Object value);
	
	public boolean isReadable();
	
	public boolean isWritable();
}
