package nz.co.gregs.dbvolution.annotations;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

import nz.co.gregs.dbvolution.DBDatabase;
import nz.co.gregs.dbvolution.datatypes.DBTypeAdaptor;
import nz.co.gregs.dbvolution.datatypes.QueryableDatatype;

/**
 * Adapts a non-DBvolution field or property to a DBvolution type, or adapts a
 * DBvolution field or property to a different DBvolution type. Where the
 * adapted field or property is a non-DBvolution type, a null value returned by
 * the type adaptor is translated into a non-null {@link QueryableDatatype} of
 * the appropriate type with {@link QueryableDatatype#isNull()} {@code true} and
 * the field is not used in the {@code WHERE} clause of queries.
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
     * Indicates that the 'type' is implied by other details.
     */
    static class Implicit extends QueryableDatatype {

        private static final long serialVersionUID = 1L;

        private Implicit() {
        }

        @Override
        public String getSQLDatatype() {
            return null;
        }

        @Override
        protected String formatValueForSQLStatement(DBDatabase db) {
            return null;
        }

        @Override
        public void setValue(Object newLiteralValue) {
        }

        @Override
        public boolean isAggregator() {
            return false;
        }
    }

    /**
     * The custom type adaptor to use to convert between the type of the
     * annotated field/property and the value of {@link #type()}.
     *
     * <p>
     * The indicated class must be able to be instantiated. It cannot be an
     * interface or an abstract class and must have a default constructor.
     *
     * @return the adaptor used to mediate between the external java object
     * (possibly a QueryableDatatype) and the internalQueryableDatatype.
     */
    Class<? extends DBTypeAdaptor<?, ?>> value();

    /**
     * The DBvolution type that the adaptor converts to.
     *
     * @return the QueryableDatatype class used internally for DB communication
     */
    Class<? extends QueryableDatatype> type() default Implicit.class;
}
