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
    @SuppressWarnings("unchecked")
	public void setValue(E enumValue) {
    	this.enumType = (enumValue == null) ? null : (Class<E>)enumValue.getClass();
    	super.setValue(convertToLiteral(enumValue));
    }
    
    /**
     * Validates whether the given type is acceptable as a literal value.
     * Enum values with null literal values are tolerated and should not be rejected
     * by this method. See documentation for {@link DBEnumValue#getCode()}.
     * @param enumValue non-null enum value, for which the literal value may be null
     * @throws IncompatibleClassChangeError on incompatible types
     */
    protected abstract void validateLiteralValue(E enumValue);
    
    /**
     * Gets the enumeration value.
     * <p>Converts in-line from the database's raw value to the enum type.
     * 
     * @return the Enumeration instance that is appropriate to this instance
     * @throws IllegalArgumentException if the database's raw value
     * does not have a corresponding value in the enum
     */
    public E enumValue() {
    	// get actual literal value: a String or a Long
        Object localValue = super.getValue();
        if (localValue == null) {
        	return null;
        }
        
        // attempt conversion
		E[] enumValues = getEnumType().getEnumConstants();
		for (E enumValue: enumValues) {
			if (enumValue instanceof DBEnumValue) {
				Object enumLiteralValue = ((DBEnumValue<?>) enumValue).getCode();
				if (areLiteralValuesEqual(localValue, enumLiteralValue)) {
					return enumValue;
				}
			}
			else {
				throw new IllegalArgumentException("Enum type "+enumType.getName()+" must implement "+DBEnumValue.class.getSimpleName());
			}
		}
		throw new IncompatibleClassChangeError("Invalid literal value ["+localValue+"] encountered" +
				" when converting to enum type "+enumType.getName());
    }
    
    /**
     * Tests whether two objects represent the same value.
     * Handles subtle differences in type.
     * @param o1
     * @param o2
     * @return {@code true} if both null or equivalent on value,
     *         {@code false} if not equal
     * @throws IncompatibleClassChangeError if can't recognise the type
     */
    private static boolean areLiteralValuesEqual(Object o1, Object o2) {
    	if (o1 == null && o2 == null) {
    		return true;
    	}
    	else if (o1 == null ^ o2 == null) {
    		return false;
    	}
    	
    	// handle same types and related types
    	// (includes support for: String, BicDecimal, BigInteger, custom types)
    	if (o1.getClass().isAssignableFrom(o2.getClass())) {
        	// t2 extends t1: assume t2 knows how to compare them
    		return o2.equals(o1);
    	}
    	else if (o2.getClass().isAssignableFrom(o1.getClass())) {
        	// t1 extends t2: assume t1 knows how to compare them
    		return o1.equals(o2);
    	}
    	
    	// handle java.lang.Number variations
    	// (Get the values at the greatest common precision,
    	//  then compare them)
    	if (o1 instanceof Number && o2 instanceof Number &&
    			isRecognisedRealOrIntegerType((Number) o1) &&
    			isRecognisedRealOrIntegerType((Number) o2)) {
    		Number n1 = (Number) o1;
    		Number n2 = (Number) o2;
    		Object v1 = null; // value at greatest common precision
    		Object v2 = null; // value at greatest common precision
    		
    		if (n1 instanceof Double || n2 instanceof Double) {
    			v1 = n1.doubleValue();
    			v2 = n2.doubleValue();
    		}
    		else if (n1 instanceof Float || n2 instanceof Float) {
    			v1 = n1.floatValue();
    			v2 = n2.floatValue();
    		}
    		else if (n1 instanceof Long || n2 instanceof Long) {
    			v1 = n1.longValue();
    			v2 = n2.longValue();
    		}
    		else if (n1 instanceof Integer || n2 instanceof Integer) {
    			v1 = n1.intValue();
    			v2 = n2.intValue();
    		}
    		else if (n1 instanceof Short || n2 instanceof Short) {
    			v1 = n1.shortValue();
    			v2 = n2.shortValue();
    		}
    		else if (n1 instanceof Float || n2 instanceof Float) {
    			v1 = n1.floatValue();
    			v2 = n2.floatValue();
    		}
    		
    		if (v1 != null && v2 != null) {
    			return v1.equals(v2);
    		}
    	}
    	
    	throw new IncompatibleClassChangeError("Unable to compare "+o1.getClass().getName()+" with "+o2.getClass().getName());
    }
    
    /**
     * Checks whether its one of the recognised types
     * that can be easily converted between each other
     * in {@link #areLiteralValuesEqual()}.
     */
    private static boolean isRecognisedRealOrIntegerType(Number n) {
    	return
    			(n instanceof Double) ||
    			(n instanceof Float) ||
    			(n instanceof Short) ||
    			(n instanceof Long) ||
    			(n instanceof Integer) ||
    			(n instanceof Short);
    }
    
    /**
     * Gets the declared type of enumeration that the literal value is to be mapped to.
     * Dependent on the property wrapper being injected, or the enumType
     * being set
     * @return non-null enum type
     * @throws IllegalStateException if not configured correctly
     */
    @SuppressWarnings("unchecked")
	private Class<E> getEnumType() {
    	if (enumType == null) {
    		if (propertyWrapper == null) {
    			throw new IllegalStateException(
    					"Unable to convert literal value to enum: enum type unable to be inferred at this point. " +
    					"Row needs to be queried from database, or value set with an actual enum.");
    		}
    		Class<?> type = propertyWrapper.getEnumType();
    		if (type == null) {
    			throw new IllegalStateException(
    					"Unable to convert literal value to enum: enum type unable to be inferred at this point. " +
    					"Row needs to be queried from database, or value set with an actual enum, "+
    					"on "+propertyWrapper.qualifiedJavaName()+".");
    		}
    		enumType = (Class<E>) type;
    	}
    	return enumType;
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

	protected Object[] convertToLiteral(E... enumValues) {
    	Object[] result = new Object[enumValues.length];
    	for (int i=0; i < enumValues.length; i++) {
    		E enumValue = enumValues[i];
    		result[i] = convertToLiteral(enumValue);
    	}
    	return result;
    }
    
    protected Object convertToLiteral(E enumValue) {
    	if (enumValue == null || enumValue.getCode() == null) {
    		return null;
    	}
    	else {
	    	Object literalValue = enumValue.getCode();
	    	validateLiteralValue(enumValue);
	    	return literalValue;
    	}
    }
}
