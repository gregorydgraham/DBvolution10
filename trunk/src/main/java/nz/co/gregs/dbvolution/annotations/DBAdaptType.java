package nz.co.gregs.dbvolution.annotations;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

import nz.co.gregs.dbvolution.datatypes.DBTypeAdaptor;
import nz.co.gregs.dbvolution.datatypes.QueryableDatatype;

/**
 * Examples:
 *
 * @DBAdaptType(adaptor=DaysSinceEpochDateAdaptor.class, type=DBDate.class)
 * public DBInteger daysSinceEpoch;
 *
 * @DBAdaptType(adaptor=MyFreeTextNumberAdaptor.class, type=DBInteger.class)
 * public String freeTextNumber;
 *
 * @DBAdaptType(adaptor=TrimmingStringAdaptor.class, type=DBString.class) public
 * DBString trimmedValue;
 */
@Target({ElementType.FIELD, ElementType.METHOD})
@Retention(RetentionPolicy.RUNTIME)
public @interface DBAdaptType {

    /**
     * The custom type adaptor to use to convert between the type of the
     * annotated field/property and the value of {@link #type()}.
     *
     * <p> The indicated class must be able to be instantiated. It cannot be an interface or an abstract class), and must have a default
     * constructor.
     *
     * @return
     */
    Class<? extends DBTypeAdaptor<?, ?>> adaptor();

    /**
     * The DBvolution type that the adaptor converts to.
     *
     * @return
     */
    Class<? extends QueryableDatatype> type() default QueryableDatatype.class;
}
