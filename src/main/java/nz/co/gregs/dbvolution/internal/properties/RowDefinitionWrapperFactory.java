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
 * <p style="color: #F90;">Support DBvolution at
 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
 *
 * @author Malcolm Lett
 */
public class RowDefinitionWrapperFactory {

	/**
	 * Thread-safety: access to this object must be synchronized on it
	 */
	private final Map<Class<?>, RowDefinitionClassWrapper> classWrappersByClass = new HashMap<Class<?>, RowDefinitionClassWrapper>();

	/**
	 * Gets the class adaptor for the given class. If an adaptor for the given
	 * class has not yet been created, one will be created and added to the
	 * internal cache.
	 *
	 * @param clazz clazz
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 * @return the class adaptor
	 */
	public RowDefinitionClassWrapper classWrapperFor(Class<? extends RowDefinition> clazz) {
		synchronized (classWrappersByClass) {
			RowDefinitionClassWrapper wrapper = classWrappersByClass.get(clazz);
			if (wrapper == null) {
				wrapper = new RowDefinitionClassWrapper(clazz);
				classWrappersByClass.put(clazz, wrapper);
			}
			return wrapper;
		}
	}

	/**
	 * Gets the object adaptor for the given object. If an adaptor for the
	 * object's class has not yet been created, one will be created and added to
	 * the internal cache.
	 *
	 * @param object the DBRow instance to wrap
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 * @return the object adaptor for the given object
	 */
	public RowDefinitionInstanceWrapper instanceWrapperFor(RowDefinition object) {
		return classWrapperFor(object.getClass()).instanceWrapperFor(object);
	}
}
