package nz.co.gregs.dbvolution.datatypes;

import nz.co.gregs.dbvolution.DBDatabase;

/**
 * Base class for enumeration-aware queryable datatypes.
 * Enumeration-aware queryable datatypes map automatically from
 * the database value to the enumeration value via
 * the {@link DBEnumValue} interface.
 * 
 * <p> Internally stores only the database-centric literal value
 * in its type. Conversion to the enumeration type is done lazily so that
 * it's possible to handle the case where a database has
 * an invalid value or a new value that isn't in the enumeration.
 * @param <E> the enumeration type. Must implement {@link DBEnumValue}.
 */
public abstract class DBEnum<E extends Enum<E> & DBEnumValue<?>> extends QueryableDatatype {
	private static final long serialVersionUID = 1L;

	// values needing to be populated somehow
	private Class<E> enumType;
	
	public DBEnum() {
    }

	protected DBEnum(Object literalValue) {
		super(literalValue);
    }
	
    public DBEnum(E value) {
        super();
        setValue(value);
    }
    
    /**
     * Sets the value based on the given enumeration.
     * @param enumValue
     */
    public void setValue(E enumValue) {
    	super.setValue(convertToLiteral(enumValue));
    }
    
	protected Object[] convertToLiteral(E... enumValues) {
    	Object[] result = new Object[enumValues.length];
    	for (int i=0; i < enumValues.length; i++) {
    		E enumValue = enumValues[i];
    		result[i] = convertToLiteral(enumValue);
    	}
    	return result;
    }
    
    protected Object convertToLiteral(E enumValue) {
    	if (enumValue == null || enumValue.getLiteralValue() == null) {
    		return null;
    	}
    	else {
	    	Object literalValue = enumValue.getLiteralValue();
	    	validateLiteralValue(enumValue);
	    	return literalValue;
    	}
    }

    /**
     * Validates whether the given type is acceptable as a literal value.
     * Enum values with null literal values are tolerated and should not be rejected
     * by this method. See documentation for {@link DBEnumValue#getLiteralValue()}.
     * @param enumValue non-null enum value, for which the literal value may be null
     * @throws IncompatibleClassChangeError on incompatible types
     */
    protected abstract void validateLiteralValue(E enumValue);
    
    /**
     * Gets the enumeration value.
     * Converts in-line from the database's raw value to the enum type.
     * If  
     * @return
     * @throws IllegalArgumentException if the database's raw value
     * does not have a corresponding value in the enum
     */
    // TODO: needs to handle where DBEnumValue.getLiteralValue() returns an Integer, and super.getValue() returns a Long, etc.
    public E enumValue() {
    	// get actual literal value: a String or a Long
        Object literalValue = super.getValue();
        if (literalValue == null) {
        	return null;
        }
        
        // attempt conversion
		E[] enumValues = enumType.getEnumConstants();
		for (E enumValue: enumValues) {
			if (enumValue instanceof DBEnumValue) {
				Object enumLiteralValue = ((DBEnumValue<?>) enumValue).getLiteralValue();
				if (literalValue.equals(enumLiteralValue)) {
					return enumValue;
				}
			}
			else {
				throw new IllegalArgumentException("Enum type "+enumType.getName()+" must implement "+DBEnumValue.class.getSimpleName());
			}
		}
		throw new IncompatibleClassChangeError("Invalid literal value ["+literalValue+"] encountered when converting to enum type "+enumType.getName());
    }

    @Override
    protected String formatValueForSQLStatement(DBDatabase db) {
        final Object databaseValue = super.getValue();
        if (databaseValue == null) {
            return db.getDefinition().getNull();
        } else {
            QueryableDatatype qdt = QueryableDatatype.getQueryableDatatypeForObject(databaseValue);
            return qdt.formatValueForSQLStatement(db);
        }
    }
    
    /**
    *
    * reduces the rows to only the object, Set, List, Array, or vararg of
    * objects
    *
    * @param permitted
    */
   public void permittedValues(E... permitted) {
       super.permittedValues(convertToLiteral(permitted));
   }

   /**
    *
    * excludes the object, Set, List, Array, or vararg of objects
    *
    *
    * @param excluded
    */
   public void excludedValues(E... excluded) {
	   super.excludedValues(convertToLiteral(excluded));
   }

   public void permittedRange(E lowerBound, E upperBound) {
       super.permittedRange(convertToLiteral(lowerBound), convertToLiteral(upperBound));
   }

   public void permittedRangeInclusive(E lowerBound, E upperBound) {
       super.permittedRangeInclusive(convertToLiteral(lowerBound), convertToLiteral(upperBound));
   }

   public void excludedRange(E lowerBound, E upperBound) {
       super.excludedRange(convertToLiteral(lowerBound), convertToLiteral(upperBound));
   }

   public void excludedRangeInclusive(E lowerBound, E upperBound) {
       super.excludedRangeInclusive(convertToLiteral(lowerBound), convertToLiteral(upperBound));
   }
}
