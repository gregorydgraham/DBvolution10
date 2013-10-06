package nz.co.gregs.dbvolution.generation.ast;

import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.nullValue;
import static org.junit.Assert.assertThat;
import nz.co.gregs.dbvolution.generation.CodeGenerationConfiguration;
import nz.co.gregs.dbvolution.generation.ast.ParsedBeanProperty.ParsedPropertyMember;

import org.junit.Test;

public class ParsedBeanPropertyTest extends AbstractASTTest {
	@Test
	public void fieldAndGetterAndSetterExistOnStandardTestProperty() {
		ParsedClass javatype = ParsedClass.parseContents(getMarqueSource());
		ParsedBeanProperty property = new ParsedBeanProperty(
				javatype.getField("numericCode"),
				javatype.getMethod("getNumericCode"),
				javatype.getMethod("setNumericCode"));
		assertThat("field", property.field(), is(not(nullValue())));
		assertThat("getter", property.getter(), is(not(nullValue())));
		assertThat("setter", property.setter(), is(not(nullValue())));
	}

	@Test
	public void propertyNameRetrievedGivenValidFieldAndGetterSetters() {
		ParsedClass javatype = ParsedClass.parseContents(getMarqueSource());
		ParsedBeanProperty property = new ParsedBeanProperty(
				javatype.getField("numericCode"),
				javatype.getMethod("getNumericCode"),
				javatype.getMethod("setNumericCode"));
	
		assertThat("property name", property.getName(), is("numericCode"));
	}
	
	@Test
	public void columnNameRetrievedGivenValidFieldAndGetterSetters() {
		ParsedClass javatype = ParsedClass.parseContents(getMarqueSource());
		ParsedBeanProperty property = new ParsedBeanProperty(
				javatype.getField("numericCode"),
				javatype.getMethod("getNumericCode"),
				javatype.getMethod("setNumericCode"));
	
		assertThat("column name", property.getColumnNameIfSet(), is("numeric_code"));
	}

	@Test
	public void propertyTypeRetrievedGivenValidFieldAndGetterSetters() {
		ParsedClass javatype = ParsedClass.parseContents(getMarqueSource());
		ParsedBeanProperty property = new ParsedBeanProperty(
				javatype.getField("numericCode"),
				javatype.getMethod("getNumericCode"),
				javatype.getMethod("setNumericCode"));
	
		assertThat("property type", property.getType().nameAstNode().toString(), is("DBNumber"));
	}
	
	@Test
	public void selectsCorrectAnnotatedMemberGivenPrivateFieldAndPublicAccessorsAndFieldConfigWithoutAnnotations() {
		CodeGenerationConfiguration config = new CodeGenerationConfiguration();
		config.setGenerateAccessorMethods(true);
		config.setAnnotateFields(true);
		ParsedClass clazz = ParsedClass.newInstance(config, "testClass");
		ParsedTypeContext typeContext = clazz.getTypeContext();
		
		ParsedField field = ParsedField.newInstance(typeContext, "value", String.class, false);
		ParsedMethod getter = ParsedMethod.newGetterInstance(typeContext, field);
		ParsedMethod setter = ParsedMethod.newSetterInstance(typeContext, field);
		
		ParsedPropertyMember member = ParsedBeanProperty.selectedAnnotatedMember(field, getter, setter);
		assertThat(member, is((ParsedPropertyMember) field));
	}
	
	@Test
	public void selectsCorrectAnnotatedMemberGivenPublicFieldAndAccessorsAndFieldConfigWithoutAnnotations() {
		CodeGenerationConfiguration config = new CodeGenerationConfiguration();
		config.setGenerateAccessorMethods(true);
		config.setAnnotateFields(true);
		ParsedClass clazz = ParsedClass.newInstance(config, "testClass");
		ParsedTypeContext typeContext = clazz.getTypeContext();
		
		ParsedField field = ParsedField.newInstance(typeContext, "value", String.class, true);
		ParsedMethod getter = ParsedMethod.newGetterInstance(typeContext, field);
		ParsedMethod setter = ParsedMethod.newSetterInstance(typeContext, field);
		
		ParsedPropertyMember member = ParsedBeanProperty.selectedAnnotatedMember(field, getter, setter);
		assertThat(member, is((ParsedPropertyMember) field));
	}
	
}
