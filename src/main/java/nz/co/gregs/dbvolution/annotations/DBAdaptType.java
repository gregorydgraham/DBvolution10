package nz.co.gregs.dbvolution.annotations;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;
import nz.co.gregs.dbvolution.datatypes.DBInteger;
import nz.co.gregs.dbvolution.datatypes.DBString;
import nz.co.gregs.dbvolution.datatypes.DBTypeAdaptor;
import nz.co.gregs.dbvolution.datatypes.DBUnknownDatatype;
import nz.co.gregs.dbvolution.datatypes.QueryableDatatype;

/**
 * Adapts a non-DBvolution field or property to a DBvolution type, or adapts a
 * DBvolution field or property to a different DBvolution type.
 *
 * <p>
 * DBAdaptType uses a {@link DBTypeAdaptor} and a {@link QueryableDatatype} such
 * as {@link DBInteger} or {@link DBString} to convert database class into a
 * different Java class
 *
 * <p>
 * In some databases values are stored in an unusual datatype. For instance a
 * date might be stored as an integer or a integer as a string. It is possible
 * to implement these as a custom QueryableDatatype but it is much easier to
 * "adapt" the value to an existing QDT. This also allows for a form of
 * pre-processing of values like the TrimmingStringAdaptor example below.
 *
 * <p>
 * Think of the &#64;DAdaptType annotation adding a bridge between the actual
 * QDT and the perceived QDT. This is similar to the bridge that QDTs provide
 * between the actual DB value and the perceived Java value.
 *
 * <p>
 * Where the adapted field or property is a non-QDT type, a null value returned
 * by the type adaptor is translated into a non-null {@link QueryableDatatype}
 * of the appropriate type with {@link QueryableDatatype#isNull()} {@code true}
 * and the field is not used in the {@code WHERE} clause of queries.
 *
 * Examples:
 * <pre>
 * &#64;DBAdaptType(value=DaysSinceEpochDateAdaptor.class, type=DBDate.class)
 * public DBInteger daysSinceEpoch;
 *
 * &#64;DBAdaptType(value=MyFreeTextNumberAdaptor.class, type=DBInteger.class)
 * public String freeTextNumber;
 *
 * &#64;DBAdaptType(value=TrimmingStringAdaptor.class, type=DBString.class) public
 * DBString trimmedValue;
 * </pre>
 */
@Target({ElementType.FIELD, ElementType.METHOD})
@Retention(RetentionPolicy.RUNTIME)
public @interface DBAdaptType {

	/**
	 * The custom type adaptor to use to convert between the type of the annotated
	 * field/property and the value of {@link #type()}.
	 *
	 * <p>
	 * The indicated class must be able to be instantiated. It cannot be an
	 * interface or an abstract class and must have a default constructor.
	 *
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 *
	 * @return the adaptor used to mediate between the external java object
	 * (possibly a QueryableDatatype) and the internalQueryableDatatype.
	 */
	Class<? extends DBTypeAdaptor<?, ?>> value();

	/**
	 * The DBvolution type that the adaptor converts to.
	 *
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 *
	 * @return the QueryableDatatype class used internally for DB communication
	 */
	Class<? extends QueryableDatatype<?>> type() default DBUnknownDatatype.class;
}
