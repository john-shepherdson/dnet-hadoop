
package eu.dnetlib.common.rmi;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * Created by claudio on 30/11/2016.
 * to be used in REST controllers, and autodiscovered to build and publish their documentation
 */
@Target({
	ElementType.TYPE
})
@Retention(RetentionPolicy.RUNTIME)
public @interface DNetRestDocumentation {

}
