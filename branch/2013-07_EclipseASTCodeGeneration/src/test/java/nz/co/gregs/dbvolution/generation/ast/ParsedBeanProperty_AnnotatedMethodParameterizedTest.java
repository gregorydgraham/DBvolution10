package nz.co.gregs.dbvolution.generation.ast;

import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.nullValue;
import static org.junit.Assert.assertThat;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import nz.co.gregs.dbvolution.generation.CodeGenerationConfiguration;
import nz.co.gregs.dbvolution.generation.ast.ParsedBeanProperty.ParsedPropertyMember;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

@RunWith(Parameterized.class)
public class ParsedBeanProperty_AnnotatedMethodParameterizedTest {
	private static final Object PRIVATE = "private";
	private static final Object PUBLIC = "public";
	private static final Object FIELD = "field";
	private static final Object GETTER = "getter";
	private static final Object SETTER = "setter";
	
	private CodeGenerationConfiguration config;
	private ParsedField field;
	private ParsedMethod getter;
	private ParsedMethod setter;
	private ParsedPropertyMember expected;

	@Parameters(name = "@{index}: config {1} - {3},{4},{5}  -> {0}") // {index} needed to ensure uniqueness
	public static Collection<Object[]> getParams() {
		List<Object[]> data = new ArrayList<Object[]>();
		
		// without annotations: field
		data.add(new Object[]{FIELD,  true,  PUBLIC,  PUBLIC, PUBLIC, null}); // field wanted
		data.add(new Object[]{FIELD,  true,  PRIVATE, PUBLIC, PUBLIC, null}); // even if field is private
		data.add(new Object[]{FIELD,  true,  PUBLIC,  null,   null,   null}); // definitely if no accessors
		data.add(new Object[]{FIELD,  true,  PRIVATE, null,   null,   null}); // definitely if no accessors
		data.add(new Object[]{FIELD,  false, PUBLIC,  null,   null,   null}); // definitely if no accessors, even if prefer accessor
		data.add(new Object[]{FIELD,  false, PRIVATE, null,   null,   null}); // definitely if no accessors, even if prefer accessor
		
		// without annotations: getter
		data.add(new Object[]{GETTER, false, PUBLIC,  PUBLIC, PUBLIC, null}); // accessor wanted
		data.add(new Object[]{GETTER, true,  null,    PUBLIC, PUBLIC, null}); // definitely if no field, even if prefer field
		data.add(new Object[]{GETTER, true,  null,    PUBLIC, null,   null}); // definitely if no field or setter

		// without annotations: setter
		data.add(new Object[]{SETTER, false, PUBLIC,  null,   PUBLIC, null}); // accessor wanted and no getter
		data.add(new Object[]{SETTER, true,  null,    null,   PUBLIC, null}); // definitely if no field, even if prefer field
		
		// with DBcolumn annotations
		data.add(new Object[]{FIELD, false, PUBLIC, PUBLIC, PUBLIC, new TestSetup(){ // existing DBColumn takes precedence
			public void addAnnotations(ParsedTypeContext typeContext, ParsedField field, ParsedMethod getter, ParsedMethod setter) {
				field.addAnnotation(ParsedDBColumnAnnotation.newInstance(typeContext, "column"));
			}
		}});
		data.add(new Object[]{GETTER, true, PUBLIC, PUBLIC, PUBLIC, new TestSetup(){ // existing DBColumn takes precedence
			public void addAnnotations(ParsedTypeContext typeContext, ParsedField field, ParsedMethod getter, ParsedMethod setter) {
				getter.addAnnotation(ParsedDBColumnAnnotation.newInstance(typeContext, "column"));
			}
		}});
		data.add(new Object[]{SETTER, true, PUBLIC, PUBLIC, PUBLIC, new TestSetup(){ // existing DBColumn takes precedence
			public void addAnnotations(ParsedTypeContext typeContext, ParsedField field, ParsedMethod getter, ParsedMethod setter) {
				setter.addAnnotation(ParsedDBColumnAnnotation.newInstance(typeContext, "column"));
			}
		}});

		// with multiple annotations
		data.add(new Object[]{FIELD, true,  PUBLIC, PUBLIC, PUBLIC, new TestSetup(){ // DBColumn + field preference
			public void addAnnotations(ParsedTypeContext typeContext, ParsedField field, ParsedMethod getter, ParsedMethod setter) {
				field.addAnnotation(ParsedDBColumnAnnotation.newInstance(typeContext, "column"));
				getter.addAnnotation(ParsedDBColumnAnnotation.newInstance(typeContext, "column"));
			}
		}});
		data.add(new Object[]{GETTER, false, PUBLIC, PUBLIC, PUBLIC, new TestSetup(){ // DBColumn + accessor preference
			public void addAnnotations(ParsedTypeContext typeContext, ParsedField field, ParsedMethod getter, ParsedMethod setter) {
				field.addAnnotation(ParsedDBColumnAnnotation.newInstance(typeContext, "column"));
				getter.addAnnotation(ParsedDBColumnAnnotation.newInstance(typeContext, "column"));
			}
		}});
		data.add(new Object[]{SETTER, false, PUBLIC, null,    PUBLIC, new TestSetup(){ // DBColumn + accessor preference
			public void addAnnotations(ParsedTypeContext typeContext, ParsedField field, ParsedMethod getter, ParsedMethod setter) {
				field.addAnnotation(ParsedDBColumnAnnotation.newInstance(typeContext, "column"));
				setter.addAnnotation(ParsedDBColumnAnnotation.newInstance(typeContext, "column"));
			}
		}});
		
		return data;
	}
	
	/**
	 * 
	 * @param expectedMemberType one of FIELD, GETTER, SETTER, or null
	 * @param generateAccessorMethods code generation configuration
	 * @param annotateFields code generation configuration
	 * @param fieldVisibility one of PRIVATE, PUBLIC, or null
	 * @param getterVisibility one of PRIVATE, PUBLIC, or null
	 * @param setterVisibility one of PRIVATE, PUBLIC, or null
	 * @param testSetup extra test setup steps as needed
	 */
	// note: assuming config.generateAccessorMethods does not have an effect
	public ParsedBeanProperty_AnnotatedMethodParameterizedTest(
			String expectedMemberType,
			boolean annotateFields,
			String fieldVisibility, String getterVisibility, String setterVisibility, TestSetup testSetup) {
		this.config = new CodeGenerationConfiguration();
		this.config.setAnnotateFields(annotateFields);
		
		ParsedClass clazz = ParsedClass.newInstance(config, "testClass");
		ParsedTypeContext typeContext = clazz.getTypeContext();
		
		ParsedField newField = ParsedField.newInstance(typeContext, "value", String.class, isPublic(fieldVisibility, true));
		if (fieldVisibility != null) {
			this.field = newField;
		}
		
		if (!isPublic(getterVisibility, true)) {
			throw new IllegalArgumentException("Getter visibility must be public at the moment");
		}
		if (!isPublic(setterVisibility, true)) {
			throw new IllegalArgumentException("Setter visibility must be public at the moment");
		}
		
		this.getter = (getterVisibility == null) ? null : ParsedMethod.newGetterInstance(typeContext, newField);
		this.setter = (setterVisibility == null) ? null : ParsedMethod.newSetterInstance(typeContext, newField);

		if (testSetup != null) {
			testSetup.addAnnotations(typeContext, field, getter, setter);
		}
		
		if (expectedMemberType == null) {
			this.expected = null;
		}
		else if (expectedMemberType.equals(FIELD)) {
			if (fieldVisibility == null) {
				throw new IllegalArgumentException("can't expect field because it's null");
			}
			this.expected = field;
		}
		else if (expectedMemberType.equals(GETTER)) {
			if (getterVisibility == null) {
				throw new IllegalArgumentException("can't expect getter because it's null");
			}
			this.expected = getter;
		}
		else if (expectedMemberType.equals(SETTER)) {
			if (setterVisibility == null) {
				throw new IllegalArgumentException("can't expect setter because it's null");
			}
			this.expected = setter;
		}
		else {
			throw new IllegalArgumentException("Not a valid expected member type: "+expectedMemberType);
		}
	}

	@Test
	public void selectedMemberIsCorrect() {
		ParsedPropertyMember member = ParsedBeanProperty.selectedAnnotatedMember(field, getter, setter);
		
		// expect null
		if (expected == null) {
			assertThat(member, is(nullValue()));
		}
		
		// expect specific member
		else {
			assertThat(member, is(expected));
		}
	}
	
	private boolean isPublic(Object classifier, Boolean defaultIfNull) {
		if (classifier == null && defaultIfNull != null) {
			return defaultIfNull;
		}
		
		if (classifier.equals(PUBLIC)) {
			return true;
		}
		else if (classifier.equals(PRIVATE)) {
			return false;
		}
		else {
			throw new IllegalArgumentException("Not a valid visibility classifier: "+classifier);
		}
	}
	
	private static interface TestSetup {
		public void addAnnotations(ParsedTypeContext typeContext, ParsedField field, ParsedMethod getter, ParsedMethod setter);
	}
}
