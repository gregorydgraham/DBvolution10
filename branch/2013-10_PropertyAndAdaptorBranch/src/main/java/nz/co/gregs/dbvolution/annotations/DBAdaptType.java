/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package nz.co.gregs.dbvolution.annotations;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * @DBAdaptType(adaptor=MyTypeAdaptorImplementationClass.class, type=DBString.class)
 * public Integer number;
 */
@Target(ElementType.FIELD)
@Retention(RetentionPolicy.RUNTIME)
public @interface DBAdaptType {
    Class<?> adaptor();
    Class<?> type();
    
}
