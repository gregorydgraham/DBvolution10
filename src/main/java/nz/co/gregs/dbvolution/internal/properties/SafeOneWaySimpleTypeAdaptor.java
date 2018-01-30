package nz.co.gregs.dbvolution.internal.properties;

import java.io.Serializable;
import java.lang.reflect.Method;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import nz.co.gregs.dbvolution.datatypes.DBTypeAdaptor;
import nz.co.gregs.dbvolution.exceptions.DBThrownByEndUserCodeException;
import nz.co.gregs.dbvolution.internal.properties.InterfaceInfo.ParameterBounds;
import nz.co.gregs.dbvolution.internal.properties.InterfaceInfo.UnsupportedType;

/**
 * Internal class that wraps one direction of a {@link DBTypeAdaptor} with type
 * checking, meaningful error messages, and automatic casting between number
 * types.
 */
// TODO exceptions need to reference the field the type adaptor is on
public class SafeOneWaySimpleTypeAdaptor implements Serializable{
	
	private static final long serialVersionUID = 1l;

	private static final Log log = LogFactory.getLog(SafeOneWaySimpleTypeAdaptor.class);

	/**
	 * Enumerates the possible directions that a QDT Sync operation can have.
	 *
	 */
	public static enum Direction {

		/**
		 * To DBvolution-centric type. toDatabaseValue() method
		 */
		TO_INTERNAL,
		/**
		 * To end-user declared type of field. fromDatabaseValue() method
		 */
		TO_EXTERNAL
	}

	private static final Method TO_EXTERNAL_METHOD;
	private static final Method TO_INTERNAL_METHOD;

	private static final SimpleCast[] SIMPLE_CASTS = {
		new NumberToShortCast(),
		new NumberToIntegerCast(),
		new NumberToLongCast(),
		new NumberToFloatCast(),
		new NumberToDoubleCast(),};

	private String propertyName;
	private Direction direction;

	private Class<?> sourceType;
	private transient SimpleCast sourceCast = null;
	private transient DBTypeAdaptor<Object, Object> typeAdaptor;
	private transient SimpleCast targetCast = null;
	private Class<?> targetType;

	static {
		try {
			TO_EXTERNAL_METHOD = DBTypeAdaptor.class.getMethod("fromDatabaseValue", Object.class);
		} catch (NoSuchMethodException e) {
			throw new RuntimeException(DBTypeAdaptor.class.getSimpleName() + " does not have a 'fromDatabaseValue' method", e);
		} catch (SecurityException e) {
			throw new RuntimeException(e);
		}

		try {
			TO_INTERNAL_METHOD = DBTypeAdaptor.class.getMethod("toDatabaseValue", Object.class);
		} catch (NoSuchMethodException e) {
			throw new RuntimeException(DBTypeAdaptor.class.getSimpleName() + " does not have a 'toDatabaseValue' method", e);
		} catch (SecurityException e) {
			throw new RuntimeException(e);
		}
	}

	/**
	 *
	 * <p>
	 * If {@code sourceType} is provided, then the inherent source type of the
	 * declared type adaptor type is checked against {@code sourceType} for
	 * compatibility.
	 *
	 * <p>
	 * If {@code targetType} is provided, then all conversions are type checked
	 * against {@code targetType} before returning from calls to
	 * {@link #convert(Object)}. {@code targetType} must be compatible with the
	 * target type inherent in the declaration of the type adaptor itself.
	 *
	 * @param propertyName propertyName
	 * @param sourceType type of variable from which input value is retrieved,
	 * optional
	 * @param direction direction
	 * @param typeAdaptor typeAdaptor
	 * @param targetType type of variable to which output value is to be assigned,
	 * optional
	 */
	@SuppressWarnings("unchecked")
	public SafeOneWaySimpleTypeAdaptor(String propertyName, DBTypeAdaptor<?, ?> typeAdaptor, Direction direction, Class<?> sourceType, Class<?> targetType) {
		this.propertyName = propertyName;
		this.direction = direction;
		this.typeAdaptor = (DBTypeAdaptor<Object, Object>) typeAdaptor;

		// infer typeAdaptor's source and target types
		try {
			InterfaceInfo interfaceInfo = new InterfaceInfo(DBTypeAdaptor.class, typeAdaptor);
			ParameterBounds[] parameterBounds = interfaceInfo.getInterfaceParameterValueBounds();

			ParameterBounds sourceBounds;
			ParameterBounds targetBounds;
			if (direction == Direction.TO_EXTERNAL) {
				sourceBounds = parameterBounds[1];
				targetBounds = parameterBounds[0];
			} else {
				sourceBounds = parameterBounds[0];
				targetBounds = parameterBounds[1];
			}

			if (sourceType != null && sourceBounds != null) {
				// sourceType must be at least one of the upper bound classes (if multi)
				boolean matched = false;
				for (Class<?> sourceBoundType : sourceBounds.upperClasses()) {
					SimpleCast cast = getSimpleCastFor(sourceType, sourceBoundType);
					if (cast != null || sourceBoundType.isAssignableFrom(sourceType)) {
						matched = true;
						this.sourceType = sourceType;
						this.sourceCast = cast;
						break;
					}
				}
				if (!matched) {
					throw new IllegalArgumentException("TypeAdaptor " + typeAdaptor.getClass().getSimpleName()
							+ " cannot be used with " + sourceType.getSimpleName() + " values");
				}
			} else if (sourceBounds != null && !sourceBounds.isUpperMulti()) {
				this.sourceType = sourceBounds.upperClass();
				//this.sourceCast = (this.sourceType == null) ? null : getSimpleCastFor(this.sourceType, null);
			}

			if (targetType != null && targetBounds != null) {
				// targetType must be at least one of the upper bound classes (if multi)
				boolean matched = false;
				for (Class<?> targetBoundType : targetBounds.upperClasses()) {
					SimpleCast cast = getSimpleCastFor(targetBoundType, targetType);
					if (cast != null || targetType.isAssignableFrom(targetBoundType)) {
						matched = true;
						this.targetCast = cast;
						this.targetType = targetType;
						break;
					}
				}
				if (!matched) {
					throw new IllegalArgumentException("TypeAdaptor " + typeAdaptor.getClass().getSimpleName()
							+ " cannot be used with " + targetType.getSimpleName() + " values");
				}
			} else if (targetBounds != null && !targetBounds.isUpperMulti()) {
				this.targetType = targetBounds.upperClass();
				//this.targetCast = (this.targetType == null) ? null : getSimpleCastFor(null, this.targetType);
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

	@Override
	public String toString() {
		StringBuilder buf = new StringBuilder();
		buf.append(sourceType == null ? "unknown" : sourceType.getSimpleName());
		buf.append("-->");
		if (sourceCast != null) {
			buf.append("(");
			buf.append(sourceCast.getClass().getSimpleName());
			buf.append(")");
			buf.append("-->");
		}
		buf.append(typeAdaptor.getClass().getSimpleName());
		buf.append("-->");
		if (targetCast != null) {
			buf.append("(");
			buf.append(targetCast.getClass().getSimpleName());
			buf.append(")");
			buf.append("-->");
		}
		buf.append(targetType == null ? "unknown" : targetType.getSimpleName());

		return buf.toString();
	}

	/**
	 * Gets the expected type of source values passed to {@link #convert(Object)}.
	 *
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 *
	 * @return null if not constrained
	 */
	public Class<?> getSourceType() {
		return sourceType;
	}

	/**
	 * Gets the type that values are converted to, possibly including extra
	 * up-casting or down-casting as needed when converting between number types.
	 *
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 *
	 * @return null if not constrained
	 */
	public Class<?> getTargetType() {
		return targetType;
	}

	/**
	 * Uses the type adaptor to convert in the configured direction.
	 *
	 * @param value value
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 * @return the value supplied converted by the type adaptor
	 * @throws ClassCastException on type conversion failure
	 * @throws DBThrownByEndUserCodeException if the type adaptor throws an
	 * exception
	 */
	public Object convert(Object value) {
		String valStr = (value == null) ? "null" : value.getClass().getSimpleName() + "[" + value + "]";
		try {
			Object result = convertInternal(value);

			String resultStr = (result == null) ? "null" : result.getClass().getSimpleName() + "[" + result + "]";
			log.debug(this + " converting " + valStr + " ==> " + resultStr);
			return result;
		} catch (RuntimeException e) {
			log.debug(this + " converting " + valStr + " ==> " + e.getClass().getSimpleName());
			throw e;
		}
	}

	private Object convertInternal(Object incomingValue) {
		Object value = incomingValue;
		// validate source
		if (sourceCast != null && value != null) {
			if (!sourceCast.acceptsSource(value)) {
				throw new ClassCastException("Cannot pass " + value.getClass().getSimpleName()
						+ " to " + methodName()
						+ ", on property " + propertyName);
			}
		} else if (sourceType != null && value != null) {
			if (!sourceType.isInstance(value)) {
				throw new ClassCastException("Cannot pass " + value.getClass().getSimpleName()
						+ " to " + methodName()
						+ ", on property " + propertyName);
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
			} catch (NullPointerException e) {
				String msg = (e.getLocalizedMessage() == null) ? "" : ": " + e.getLocalizedMessage();
				throw new DBThrownByEndUserCodeException("Type adaptor " + typeAdaptor.getClass().getSimpleName() + " threw " + e.getClass().getSimpleName()
						+ " when getting property " + propertyName + msg + ": Please ensure that the fromDatabaseValue method handles database NULLs as well as normal values.", e);
			} catch (RuntimeException e) {
				String msg = (e.getLocalizedMessage() == null) ? "" : ": " + e.getLocalizedMessage();
				throw new DBThrownByEndUserCodeException("Type adaptor threw " + e.getClass().getSimpleName()
						+ " when getting property " + propertyName + msg, e);
			}
		} else {
			try {
				result = typeAdaptor.toDatabaseValue(value);
			} catch (NullPointerException e) {
				String msg = (e.getLocalizedMessage() == null) ? "" : ": " + e.getLocalizedMessage();
				throw new DBThrownByEndUserCodeException("Type adaptor " + typeAdaptor.getClass().getSimpleName() + " threw " + e.getClass().getSimpleName()
						+ " when setting property " + propertyName + msg + ": Please ensure that the toDatabaseValue method handles database NULLs as well as normal values.", e);
			} catch (RuntimeException e) {
				String msg = (e.getLocalizedMessage() == null) ? "" : ": " + e.getLocalizedMessage();
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
				throw new ClassCastException("Cannot cast " + result.getClass().getSimpleName()
						+ " to " + targetType.getSimpleName()
						+ ", on property " + propertyName);
			}
		}
		return result;
	}

	private String methodName() {
		if (direction == Direction.TO_EXTERNAL) {
			return typeAdaptor.getClass().getSimpleName() + "." + TO_EXTERNAL_METHOD.getName() + "()";
		} else {
			return typeAdaptor.getClass().getSimpleName() + "." + TO_INTERNAL_METHOD.getName() + "()";
		}
	}

	/**
	 * Gets the appropriate simple cast or null if one doesn't exist
	 *
	 * @param sourceType the required source type, null to select by targetType
	 * only
	 * @param targetType the required target type, null to select by sourceType
	 * only
	 */
	static SimpleCast getSimpleCastFor(Class<?> sourceType, Class<?> targetType) {
		if (sourceType == null && targetType == null) {
			throw new NullPointerException("at least one of sourceType or targetType must be specified");
		}
		for (SimpleCast cast : SIMPLE_CASTS) {
			if ((sourceType == null || cast.sourceType().isAssignableFrom(sourceType))
					&& (targetType == null || targetType.isAssignableFrom(cast.targetType()))) {
				return cast;
			}
		}
		return null;
	}

	/**
	 * Used internally to handle automatic casting
	 */
	static interface SimpleCast {

		public boolean acceptsSource(Object value);

		public boolean acceptsSource(Class<?> type);

		public Object cast(Object value);

		public Class<?> sourceType();

		public Class<?> targetType();
	}

	private abstract static class BaseSimpleCast<S, T> implements SimpleCast {

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
		public String toString() {
//			StringBuilder buf = new StringBuilder();
//			buf.append(sourceType == null ? "null" : sourceType.getSimpleName());
//			buf.append("-->");
//			buf.append(targetType == null ? "null" : targetType.getSimpleName());
//			return buf.toString();
			return getClass().getSimpleName();
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
		public boolean acceptsSource(Object value) {
			if (value == null) {
				return true;
			}
			return acceptsSource(value.getClass());
		}

		@Override
		public boolean acceptsSource(Class<?> type) {
			return sourceType.isAssignableFrom(type);
		}

		@Override
		public final Object cast(Object value) {
			if (value == null) {
				return null;
			}
			if (!sourceType().isInstance(value)) {
				throw new ClassCastException("Cannot cast " + value.getClass().getSimpleName() + " to " + sourceType().getSimpleName());
			}

			@SuppressWarnings("unchecked")
			T result = safeNonNullCast((S) value);

			if (!targetType().isInstance(result)) {
				throw new ClassCastException("Cannot cast " + result.getClass().getSimpleName() + " to " + targetType().getSimpleName());
			}
			return result;
		}

		protected abstract T safeNonNullCast(S value);
	}

	static class NumberToShortCast extends BaseSimpleCast<Number, Short> {

		@Override
		protected Short safeNonNullCast(Number value) {
			return value.shortValue();
		}
	}

	static class NumberToIntegerCast extends BaseSimpleCast<Number, Integer> {

		@Override
		protected Integer safeNonNullCast(Number value) {
			return value.intValue();
		}
	}

	static class NumberToLongCast extends BaseSimpleCast<Number, Long> {

		@Override
		protected Long safeNonNullCast(Number value) {
			return value.longValue();
		}
	}

	static class NumberToFloatCast extends BaseSimpleCast<Number, Float> {

		@Override
		protected Float safeNonNullCast(Number value) {
			return value.floatValue();
		}
	}

	static class NumberToDoubleCast extends BaseSimpleCast<Number, Double> {

		@Override
		protected Double safeNonNullCast(Number value) {
			return value.doubleValue();
		}
	}
}
