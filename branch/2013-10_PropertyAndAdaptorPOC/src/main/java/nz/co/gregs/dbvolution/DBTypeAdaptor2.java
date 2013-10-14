package nz.co.gregs.dbvolution;

/**
 * Alternative possible version of the type adaptor interface.
 * Translates between a target object type and the simple type equivalent of
 * the DBvolution type.
 * 
 * <p> This approach means that DBv has more control and can look after the dirty
 * flag and permissable/excluded values itself.
 * @param <T> the target object type
 * @param <Q> the DBvolution-side type: one of String, Integer, Long, Number, Boolean, Double, Float, ByteArray, InputStream
 */
public interface DBTypeAdaptor2<T, D> {
	/**
	 * Null values must be handled correctly.
	 * @param dbvValue
	 * @return
	 */
	public T toObjectValue(D dbvValue);

	/**
	 * Null values must be handled correctly.
	 * @param objectValue
	 * @return
	 */
	public D toDBvValue(T objectValue);
}
