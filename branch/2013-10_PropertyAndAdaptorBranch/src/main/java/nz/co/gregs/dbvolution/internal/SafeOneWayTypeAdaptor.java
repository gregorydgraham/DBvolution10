package nz.co.gregs.dbvolution.internal;

import java.lang.reflect.Method;

import nz.co.gregs.dbvolution.datatypes.DBTypeAdaptor;
import nz.co.gregs.dbvolution.exceptions.DBThrownByEndUserCodeException;
import nz.co.gregs.dbvolution.internal.InterfaceInfo.ParameterBounds;
import nz.co.gregs.dbvolution.internal.InterfaceInfo.UnsupportedType;

/**
 * Internal class that wraps one direction of a {@link DBTypeAdaptor}
 * with type checking, meaningful error messages, and automatic
 * casting between number types.
 */
// TODO exceptions need to reference the field the type adaptor is on
public class SafeOneWayTypeAdaptor {
	public static enum Direction {
		/** To DBvolution-centric type. toDatabaseValue() method */
		TO_INTERNAL,
		
		/** To end-user declared type of field. fromDatabaseValue() method */
		TO_EXTERNAL
	}

	private static final Method toExternalMethod;
	private static final Method toInternalMethod;
	
	private static final SimpleCast[] SIMPLE_CASTS = {
		new NumberToShortCast(),
		new NumberToIntegerCast(),
		new NumberToLongCast(),
		new NumberToFloatCast(),
		new NumberToDoubleCast(),
	};
	
	private String propertyName;
	private Class<?> sourceType;
	private SimpleCast sourceCast = null;
	private SimpleCast targetCast = null;
	private Class<?> targetType;
	private DBTypeAdaptor<Object, Object> typeAdaptor;
	private Direction direction;
	
	static {
		try {
			toExternalMethod = DBTypeAdaptor.class.getMethod("fromDatabaseValue", Object.class);
		} catch (NoSuchMethodException e) {
			throw new RuntimeException(DBTypeAdaptor.class.getSimpleName()+" does not have a 'fromDatabaseValue' method", e);
		} catch (SecurityException e) {
			throw new RuntimeException(e);
		}

		try {
			toInternalMethod = DBTypeAdaptor.class.getMethod("toDatabaseValue", Object.class);
		} catch (NoSuchMethodException e) {
			throw new RuntimeException(DBTypeAdaptor.class.getSimpleName()+" does not have a 'toDatabaseValue' method", e);
		} catch (SecurityException e) {
			throw new RuntimeException(e);
		}
	}
	
	/**
	 * 
	 * <p> If {@code sourceType} is provided, then the inherent source type of the
	 * declared type adaptor type is checked against {@code sourceType} for compatibility.
	 * 
	 * <p> If {@code targetType} is provided, then all conversions are type checked against
	 * {@code targetType} before returning from calls to {@link #convert(Object)}.
	 * {@code targetType} must be compatible with the target type inherent
	 * in the declaration of the type adaptor itself.
	 * @param propertyName
	 * @param typeAdaptor
	 * @param direction
	 * @param sourceType type of variable from which input value is retrieved, optional
	 * @param targetType type of variable to which output value is to be assigned, optional
	 */
	@SuppressWarnings("unchecked")
	public SafeOneWayTypeAdaptor(String propertyName, DBTypeAdaptor<?,?> typeAdaptor, Direction direction, Class<?> sourceType, Class<?> targetType) {
		this.propertyName = propertyName;
		this.direction = direction;
		this.typeAdaptor = (DBTypeAdaptor<Object, Object>) typeAdaptor;
		
		// infer typeAdaptor's source and target types
        try {
            InterfaceInfo interfaceInfo = new InterfaceInfo(DBTypeAdaptor.class, typeAdaptor);
            ParameterBounds[] parameterBounds = interfaceInfo.getInterfaceParameterValueBounds();
            
            ParameterBounds sourceBounds = null;
            ParameterBounds targetBounds = null;
        	if (direction == Direction.TO_EXTERNAL) {
        		sourceBounds = parameterBounds[1];
        		targetBounds = parameterBounds[0];
        	}
        	else {
        		sourceBounds = parameterBounds[0];
        		targetBounds = parameterBounds[1];
        	}
        	
        	if (sourceType != null && sourceBounds != null) {
        		// sourceType must be at least one of the upper bound classes (if multi)
        		boolean matched = false;
    			for (Class<?> sourceBoundType: sourceBounds.upperClasses()) {
    				SimpleCast cast = getSimpleCastFor(sourceType, sourceBoundType);
    				if (cast != null || sourceBoundType.isAssignableFrom(sourceType)) {
    					matched = true;
    	    			this.sourceType = sourceType;
    	    			this.sourceCast = cast;
    					break;
    				}
    			}
    			if (!matched) {
    				throw new IllegalArgumentException("TypeAdaptor "+typeAdaptor.getClass().getSimpleName()+
    						" cannot be used with "+sourceType.getSimpleName()+" values");
    			}
        	}
        	else if (sourceBounds != null && !sourceBounds.isUpperMulti()) {
        		this.sourceType = sourceBounds.upperClass();
            }
        	
        	if (targetType != null && targetBounds != null) {
        		// targetType must be at least one of the upper bound classes (if multi)
        		boolean matched = false;
    			for (Class<?> targetBoundType: targetBounds.upperClasses()) {
    				SimpleCast cast = getSimpleCastFor(targetBoundType, targetType);
    				if (cast != null || targetType.isAssignableFrom(targetBoundType)) {
    					matched = true;
    					this.targetCast = cast;
    					this.targetType = targetType;
    					break;
    				}
    			}
    			if (!matched) {
    				throw new IllegalArgumentException("TypeAdaptor "+typeAdaptor.getClass().getSimpleName()+
    						" cannot be used with "+targetType.getSimpleName()+" values");
    			}
        	}
        	else if (targetBounds != null && !targetBounds.isUpperMulti()) {
        		this.targetType = targetBounds.upperClass();
            }
        	
        } catch (UnsupportedType dropped) {
            // bumped into generics that can't be handled, so best to give the
            // end-user the benefit of doubt and just skip the validation
//            logger.debug("Cancelled validation on type adaptor " + typeAdaptorClass.getName()
//                    + " due to internal error: " + dropped.getMessage(), dropped);
        } catch (UnsupportedOperationException dropped) {
            // bumped into generics that can't be handled, so best to give the
            // end-user the benefit of doubt and just skip the validation
//            logger.debug("Cancelled validation on type adaptor " + typeAdaptorClass.getName()
//                    + " due to internal error: " + dropped.getMessage(), dropped);
        }
	}
	
	/**
	 * Gets the expected type of source values passed
	 * to {@link #convert(Object)}.
	 * @return null if not constrained
	 */
	public Class<?> getSourceType() {
		return sourceType;
	}
	
	/**
	 * Gets the type that values are converted to,
	 * possibly including extra up-casting or down-casting as needed
	 * when converting between number types.
	 * @return null if not constrained
	 */
	public Class<?> getTargetType() {
		return targetType;
	}
	
	/**
	 * Uses the type adaptor to convert in the configured direction.
	 * @param value
	 * @return
	 * @throws ClassCastException on type conversion failure
	 * @throws DBThrownByEndUserCodeException if the type adaptor throws an exception
	 */
	public Object convert(Object value) {
		// validate source
		if (sourceType != null && value != null) {
			if (!sourceType.isInstance(value)) {
				throw new ClassCastException("Cannot pass "+value.getClass().getSimpleName()+" to "+methodName());
			}
		}
		
		// cast
		if (sourceCast != null) {
			value = sourceCast.cast(value);
		}
		
		// convert via type adaptor
		Object result;
		if (direction == Direction.TO_EXTERNAL) {
			try {
				result = typeAdaptor.fromDatabaseValue(value);
			} catch (RuntimeException e) {
				String msg = (e.getLocalizedMessage() == null) ? "" : ": "+e.getLocalizedMessage();
		        throw new DBThrownByEndUserCodeException("Type adaptor threw " + e.getClass().getSimpleName()
		                + " when getting property " + propertyName + msg, e);
			}
		}
		else {
			try {
				result = typeAdaptor.toDatabaseValue(value);
			} catch (RuntimeException e) {
				String msg = (e.getLocalizedMessage() == null) ? "" : ": "+e.getLocalizedMessage();
	            throw new DBThrownByEndUserCodeException("Type adaptor threw " + e.getClass().getSimpleName()
	                    + " when setting property " + propertyName + msg, e);
			}
		}

		// cast
		if (targetCast != null) {
			result = targetCast.cast(result);
		}
		
		// validate result
		if (targetType != null && result != null) {
			if (!targetType.isInstance(result)) {
				throw new ClassCastException("Cannot cast "+result.getClass().getSimpleName()+" to "+targetType.getSimpleName());
			}
		}
		return result;
	}
	
	private String methodName() {
		if (direction == Direction.TO_EXTERNAL) {
			return typeAdaptor.getClass().getSimpleName()+"."+toExternalMethod.getName()+"()";
		}
		else {
			return typeAdaptor.getClass().getSimpleName()+"."+toInternalMethod.getName()+"()";
		}
	}
	
	/** Gets the appropriate simple cast or null if one doesn't exist */
	static SimpleCast getSimpleCastFor(Class<?> sourceType, Class<?> targetType) {
		for (SimpleCast cast: SIMPLE_CASTS) {
			if (cast.sourceType().isAssignableFrom(sourceType) &&
					targetType.isAssignableFrom(cast.targetType())) {
				return cast;
			}
		}
		return null;
	}
	
	/**
	 * Used internally to handle automatic casting
	 */
	static interface SimpleCast {
		public Object cast(Object value);
		public Class<?> sourceType();
		public Class<?> targetType();
	}
	
	private abstract static class BaseSimpleCast<S,T> implements SimpleCast {
		private Class<?> sourceType;
		private Class<?> targetType;
		
		public BaseSimpleCast() {
            InterfaceInfo interfaceInfo = new InterfaceInfo(BaseSimpleCast.class, this);
            ParameterBounds[] parameterBounds = interfaceInfo.getInterfaceParameterValueBounds();
            try {
				sourceType = parameterBounds[0].upperClass();
	            targetType = parameterBounds[1].upperClass();
			} catch (UnsupportedType unexpected) {
				// not ever expecting this to occur
				throw new RuntimeException(unexpected);
			}
		}

		@Override
		public Class<?> sourceType() {
			return sourceType;
		}

		@Override
		public Class<?> targetType() {
			return targetType;
		}
		
		@Override
		public final Object cast(Object value) {
			if (value == null) {
				return null;
			}
			if (!sourceType().isInstance(value)) {
				throw new ClassCastException("Cannot cast "+value.getClass().getSimpleName()+" to "+sourceType().getSimpleName());
			}
			
			@SuppressWarnings("unchecked")
			T result = safeNonNullCast((S)value);
			
			if (!targetType().isInstance(result)) {
				throw new ClassCastException("Cannot cast "+result.getClass().getSimpleName()+" to "+targetType().getSimpleName());
			}
			return result;
		}
		
		protected abstract T safeNonNullCast(S value);
	}

	static class NumberToShortCast extends BaseSimpleCast<Number,Short> {
		@Override
		protected Short safeNonNullCast(Number value) {
			return value.shortValue();
		}
	}
	
	static class NumberToIntegerCast extends BaseSimpleCast<Number,Integer> {
		@Override
		protected Integer safeNonNullCast(Number value) {
			return value.intValue();
		}
	}

	static class NumberToLongCast extends BaseSimpleCast<Number,Long> {
		@Override
		protected Long safeNonNullCast(Number value) {
			return value.longValue();
		}
	}

	static class NumberToFloatCast extends BaseSimpleCast<Number,Float> {
		@Override
		protected Float safeNonNullCast(Number value) {
			return value.floatValue();
		}
	}

	static class NumberToDoubleCast extends BaseSimpleCast<Number,Double> {
		@Override
		protected Double safeNonNullCast(Number value) {
			return value.doubleValue();
		}
	}
}
