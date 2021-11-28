package nz.co.gregs.dbvolution.internal.properties;

import java.util.HashMap;
import java.util.Map;
import nz.co.gregs.dbvolution.query.RowDefinition;

/**
 * Constructs class adaptors for DB table classes and maintains an in-memory
 * cache for re-use. Creating class adaptors is expensive and this class is
 * provided as a convenience for anything that needs to access class adaptors
 * for multiple types and would benefit from the performance improvement of
 * caching their values.
 *
 * <p>
 * Note that class adaptors are immutable, so this is safe to do.
 *
 * <p>
 * This class is <i>thread-safe</i>.
 *
 * @author Malcolm Lett
 */
public class RowDefinitionWrapperFactory {

	/**
	 * Thread-safety: access to this object must be synchronized on it
	 */
	private final Map<Class<?>, RowDefinitionClassWrapper<?>> classWrappersByClass = new HashMap<Class<?>, RowDefinitionClassWrapper<?>>();

	/**
	 * Gets the class adaptor for the given class.If an adaptor for the given
	 * class has not yet been created, one will be created and added to the
	 * internal cache.
	 *
	 * @param <ROW> the type of DBRow this object applies to.
	 * @param clazz clazz
	 * @return the class adaptor
	 */
	public <ROW extends RowDefinition> RowDefinitionClassWrapper<ROW> classWrapperFor(Class<ROW> clazz) {
		synchronized (classWrappersByClass) {
			@SuppressWarnings("unchecked")
			RowDefinitionClassWrapper<ROW> wrapper = (RowDefinitionClassWrapper<ROW>) classWrappersByClass.get(clazz);
			if (wrapper == null) {
				wrapper = new RowDefinitionClassWrapper<>(clazz);
				classWrappersByClass.put(clazz, wrapper);
			}
			return wrapper;
		}
	}

	/**
	 * Gets the object adaptor for the given object.If an adaptor for the
 object's class has not yet been created, one will be created and added to
 the internal cache.
	 *
	 * @param <ROW> the type of DBRow this object applies to.
	 * @param object the DBRow instance to wrap
	 * @return the object adaptor for the given object
	 */
	public <ROW extends RowDefinition> RowDefinitionInstanceWrapper<ROW> instanceWrapperFor(ROW object) {
		@SuppressWarnings("unchecked")
		final RowDefinitionClassWrapper<ROW> classWrapper = (RowDefinitionClassWrapper<ROW>) classWrapperFor(object.getClass());
		return classWrapper.instanceWrapperFor(object);
	}
}
