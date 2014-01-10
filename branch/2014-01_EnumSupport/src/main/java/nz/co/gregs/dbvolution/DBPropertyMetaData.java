package nz.co.gregs.dbvolution;

import nz.co.gregs.dbvolution.datatypes.DBEnumValue;

// part 2: instead of passing in PropertyWrappers into DBDefinition and QDT methods,
// consider passing something like this instead.
// PropertyWrapper will implement this interface.
//
// This interface should be passed to classes that end-users could extend because
// * PropertyWrapper has a number of methods that modify the value of the underlying QDT
// * exposing all of PropertyWrapper makes it easy to cause infinite loops.
//
// Examples of infinite loops:
// public String DBEnum.getSQLDatatype(PropertyWrapper property) {
//     property.getQueryableDatatype().getSQLDatatype(property):
// }
public interface DBPropertyMetaData {
	/**
	 * Gets the enum type, or null if not appropriate
	 * @return the enum type, which should also implement {@link DBEnumValue}
	 */
	public Class<? extends Enum<?>> getEnumType();
	
	/**
	 * Gets the type of the code supplied by enum values.
	 * This is derived from the {@link DBEnumValue} implementation in the enum.
	 * @return null if not known or not appropriate
	 */
	public Class<?> getEnumCodeType();
}
