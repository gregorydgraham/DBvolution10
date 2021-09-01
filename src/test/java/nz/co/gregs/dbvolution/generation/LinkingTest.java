/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package nz.co.gregs.dbvolution.generation;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;
import java.lang.reflect.Modifier;
import org.burningwave.core.assembler.ComponentContainer;
import org.burningwave.core.assembler.ComponentSupplier;
import org.burningwave.core.classes.*;
import static org.hamcrest.CoreMatchers.*;
import static org.hamcrest.MatcherAssert.assertThat;
import org.junit.Test;

/**
 *
 * @author gregorygraham
 */
public class LinkingTest {

	public LinkingTest() {
	}

	@Test
	public void testLinkingClassesWithinACompilation() {
		UnitSourceGenerator generator = UnitSourceGenerator.create("yadayada");
		generator.addClass(getLinkedClass());
		generator.addClass(getOriginalClass());
		String made = generator.make();
//		System.out.println("\nGenerated code:\n" + made);
		//With this we store the generated source to a path
		generator.storeToClassPath(System.getProperty("user.home") + "/burningwave/temp");
		ComponentSupplier componentSupplier = ComponentContainer.getInstance();
		ClassFactory classFactory = componentSupplier.getClassFactory();
		//this method compile all compilation units and upload the generated classes to default
		//class loader declared with property "class-factory.default-class-loader" in 
		//burningwave.properties file (see "Overview and configuration").
		//If you need to upload the class to another class loader use
		//loadOrBuildAndDefine(LoadOrBuildAndDefineConfig) method
		ClassFactory.ClassRetriever classRetriever = classFactory.loadOrBuildAndDefine(generator);
		assertThat(classRetriever.getAllCompiledClasses().size(), is(2));
	}

	private ClassSourceGenerator[] getLinkedClass() {
		ClassSourceGenerator linkedClass = ClassSourceGenerator
				.create(TypeDeclarationSourceGenerator.create("LinkedClass"));

		VariableSourceGenerator newField = VariableSourceGenerator
				.create(Integer.class, "linkedField")
				.addModifier(Modifier.PUBLIC)
				.setValue("0");
		
		newField.addAnnotation(
				AnnotationSourceGenerator.create(ClassReferencingAnnotation.class)
						.addParameter(
								VariableSourceGenerator.create("value").setValue("AOriginalClass.class"))
		);
		
		linkedClass.addField(newField);
		
		return new ClassSourceGenerator[]{linkedClass};
	}

	private ClassSourceGenerator[] getOriginalClass() {
		ClassSourceGenerator originalClass = ClassSourceGenerator
				.create(TypeDeclarationSourceGenerator.create("AOriginalClass"));
		
		VariableSourceGenerator newField = VariableSourceGenerator
				.create(Integer.class, "unlinkedField")
				.addModifier(Modifier.PUBLIC)
				.setValue("0");
		
		originalClass.addField(newField);
		
		return new ClassSourceGenerator[]{originalClass};
	}

	@Target({ElementType.FIELD, ElementType.METHOD})
	@Retention(RetentionPolicy.RUNTIME)
	public static @interface ClassReferencingAnnotation {

		Class<?> value();

	}

}
