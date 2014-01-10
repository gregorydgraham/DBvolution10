package nz.co.gregs.dbvolution.annotations;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * <pre>
 * \@DBEnumType(MyEnumType.class)
 * public DBEnum&lt;MyEnumType&gt; myColumn = new DBEnum&lt;MyEnumType&gt;();
 * </pre>
 */
@Target({ElementType.FIELD, ElementType.METHOD})
@Retention(RetentionPolicy.RUNTIME)
public @interface DBEnumType {
    Class<? extends Enum<?>> value();
}
