package nz.co.gregs.dbvolution.datatypes;

import nz.co.gregs.dbvolution.DBDatabase;
import nz.co.gregs.dbvolution.internal.PropertyWrapper;

/**
 * Experimental representation of an enum-aware QDT.
 * Must be used in conjunction with either a type adaptor an a {@link @DBEnumType} annotation.
 * @param <E>
 */
public class DBEnum<E extends Enum<E> & DBEnumValue> extends QueryableDatatype {
	
	public DBEnum() {
	}
	
	public DBEnum(E value) {
		super(value);
	}
	
	@SuppressWarnings("unchecked")
	public E enumValue() {
		return (E)super.getValue();
	}

	/**
	 * There are number ways of handling the different database types in use:
	 * <ul>
	 * <li> create separate DBIntegerEnum and DBStringEnum which respectively extend DBInteger and DBString
	 * <li> modify DBvolution's internals to work off PropertyWrapper instances instead of QDT instances so
	 * that it has the extra meta-information available when analysing the property
	 * <li> inject all per-property meta-info into QDT instances before using them 
	 * </ul>
	 */
	//@Override
	public String getSQLDatatype(PropertyWrapper property) {
		if (property.getEnumCodeType() == Integer.class) {
			return new DBInteger().getSQLDatatype();
		}
		else if (property.getEnumCodeType() == String.class) {
			return new DBString().getSQLDatatype();
		}
		else {
			throw new UnsupportedOperationException("Unsupported enum code type "+property.getEnumCodeType());
		}
	}
	
	//@Override
	protected String formatValueForSQLStatement(DBDatabase db, PropertyWrapper property) {
		DBEnumValue enumValue = (DBEnumValue)enumValue();
		QueryableDatatype qdt;
		if (property.getEnumCodeType() == Integer.class) {
			qdt = (enumValue == null) ? new DBInteger() : new DBInteger((Integer)enumValue.getCode());
		}
		else if (property.getEnumCodeType() == String.class) {
			qdt = (enumValue == null) ? new DBString() : new DBString((String)enumValue.getCode());
		}
		else {
			throw new UnsupportedOperationException("Unsupported enum code type "+property.getEnumCodeType());
		}
		
		return qdt.formatValueForSQLStatement(db);
	}
}
