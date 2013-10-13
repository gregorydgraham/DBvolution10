package nz.co.gregs.dbvolution.internal;

import static nz.co.gregs.dbvolution.internal.PropertyMatchers.*;
import static org.hamcrest.Matchers.*;
import static org.junit.Assert.*;

import java.io.Serializable;
import java.util.List;

import nz.co.gregs.dbvolution.internal.JavaPropertyFinder.PropertyType;
import nz.co.gregs.dbvolution.internal.JavaPropertyFinder.Visibility;

import org.junit.Test;

public class JavaPropertyTest {
	private JavaPropertyFinder privateFieldPublicBeanFinder = new JavaPropertyFinder(
			Visibility.PRIVATE, Visibility.PUBLIC, null, (PropertyType[])null);

	
	// check basic field and property retrieval
	
	@Test
	public void getsPublicField() {
		List<JavaProperty> properties = privateFieldPublicBeanFinder.getPropertiesOf(SimpleIndependentFieldsAndPropertiesClass.class);
		assertThat(properties, hasItem(hasJavaPropertyName("publicField")));
	}

	@Test
	public void getsProtectedField() {
		List<JavaProperty> properties = privateFieldPublicBeanFinder.getPropertiesOf(SimpleIndependentFieldsAndPropertiesClass.class);
		assertThat(properties, hasItem(hasJavaPropertyName("privateField")));
	}
	
	@Test
	public void getsPrivateField() {
		List<JavaProperty> properties = privateFieldPublicBeanFinder.getPropertiesOf(SimpleIndependentFieldsAndPropertiesClass.class);
		assertThat(properties, hasItem(hasJavaPropertyName("privateField")));
	}
	
	@Test
	public void getsPublicProperty() {
		List<JavaProperty> properties = privateFieldPublicBeanFinder.getPropertiesOf(SimpleIndependentFieldsAndPropertiesClass.class);
		assertThat(properties, hasItem(hasJavaPropertyName("publicProperty")));
	}

	// doesn't work at present because can't find non-public bean-properties
	@Test
	public void cantGetProtectedProperty() {
		List<JavaProperty> properties = privateFieldPublicBeanFinder.getPropertiesOf(SimpleIndependentFieldsAndPropertiesClass.class);
		assertThat(properties, not(hasItem(hasJavaPropertyName("protectedProperty"))));
	}
	
	// doesn't work at present because can't find non-public bean-properties
	@Test
	public void cantGetPrivateProperty() {
		List<JavaProperty> properties = privateFieldPublicBeanFinder.getPropertiesOf(SimpleIndependentFieldsAndPropertiesClass.class);
		assertThat(properties, not(hasItem(hasJavaPropertyName("privateProperty"))));
	}
	
	// finding non-public bean-properties not supported yet
	@Test(expected=UnsupportedOperationException.class)
	public void errorsTryingToGetPrivateProperties() {
		JavaPropertyFinder finder = new JavaPropertyFinder(
				Visibility.PRIVATE, Visibility.PRIVATE, null, (PropertyType[])null);
		finder.getPropertiesOf(SimpleIndependentFieldsAndPropertiesClass.class);
	}
	
	
	// check shadowing effects
	
	@Test
	public void getsShadowingPrivateFieldGivenStandardBean() {
		List<JavaProperty> properties = privateFieldPublicBeanFinder.getPropertiesOf(SimpleStandardBeanClass.class);
		assertThat(properties, hasItem(allOf(hasJavaPropertyName("property"), isJavaPropertyField())));
	}

	@Test
	public void getsShadowingPublicPropertyGivenStandardBean() {
		List<JavaProperty> properties = privateFieldPublicBeanFinder.getPropertiesOf(SimpleStandardBeanClass.class);
		assertThat(properties, hasItem(allOf(hasJavaPropertyName("property"), not(isJavaPropertyField()))));
	}
	
	
	// check visibility control
	
	@Test
	public void getsPublicFieldGivenPublicOnlyFieldVisibility() {
		JavaPropertyFinder finder = new JavaPropertyFinder(
				Visibility.PUBLIC, Visibility.PUBLIC, null, (PropertyType[])null);
		
		List<JavaProperty> properties = finder.getPropertiesOf(SimpleIndependentFieldsAndPropertiesClass.class);
		assertThat(properties, hasItem(hasJavaPropertyName("publicField")));
	}

	@Test
	public void getsProtectedFieldGivenProtectedFieldVisibility() {
		JavaPropertyFinder finder = new JavaPropertyFinder(
				Visibility.PROTECTED, Visibility.PUBLIC, null, (PropertyType[])null);
		
		List<JavaProperty> properties = finder.getPropertiesOf(SimpleIndependentFieldsAndPropertiesClass.class);
		assertThat(properties, hasItem(hasJavaPropertyName("protectedField")));
	}

	@Test
	public void getsProtectedFieldGivenDefaultFieldVisibility() {
		JavaPropertyFinder finder = new JavaPropertyFinder(
				Visibility.DEFAULT, Visibility.PUBLIC, null, (PropertyType[])null);
		
		List<JavaProperty> properties = finder.getPropertiesOf(SimpleIndependentFieldsAndPropertiesClass.class);
		assertThat(properties, hasItem(hasJavaPropertyName("protectedField")));
	}
	
	@Test
	public void cantGetProtectedFieldGivenPublicOnlyFieldVisibility() {
		JavaPropertyFinder finder = new JavaPropertyFinder(
				Visibility.PUBLIC, Visibility.PUBLIC, null, (PropertyType[])null);
		
		List<JavaProperty> properties = finder.getPropertiesOf(SimpleIndependentFieldsAndPropertiesClass.class);
		assertThat(properties, not(hasItem(hasJavaPropertyName("privateField"))));
	}
	
	@Test
	public void cantGetPrivateFieldGivenPublicOnlyFieldVisibility() {
		JavaPropertyFinder finder = new JavaPropertyFinder(
				Visibility.PUBLIC, Visibility.PUBLIC, null, (PropertyType[])null);
		
		List<JavaProperty> properties = finder.getPropertiesOf(SimpleIndependentFieldsAndPropertiesClass.class);
		assertThat(properties, not(hasItem(hasJavaPropertyName("privateField"))));
	}

	@Test
	public void cantGetPrivateFieldGivenProtectedFieldVisibility() {
		JavaPropertyFinder finder = new JavaPropertyFinder(
				Visibility.PROTECTED, Visibility.PUBLIC, null, (PropertyType[])null);
		
		List<JavaProperty> properties = finder.getPropertiesOf(SimpleIndependentFieldsAndPropertiesClass.class);
		assertThat(properties, not(hasItem(hasJavaPropertyName("privateField"))));
	}
	
	@Test
	public void cantGetPrivateFieldGivenDefaultFieldVisibility() {
		JavaPropertyFinder finder = new JavaPropertyFinder(
				Visibility.DEFAULT, Visibility.PUBLIC, null, (PropertyType[])null);
		
		List<JavaProperty> properties = finder.getPropertiesOf(SimpleIndependentFieldsAndPropertiesClass.class);
		assertThat(properties, not(hasItem(hasJavaPropertyName("privateField"))));
	}
	
	
	// check handling in unusual situations
	
	@Test
	public void getsAllPropertiesWithoutExceptionGivenWeirdTypes() {
		privateFieldPublicBeanFinder.getPropertiesOf(WeirdTypesClass.class);
	}

	@Test
	public void getsArrayProperty() {
		List<JavaProperty> properties = privateFieldPublicBeanFinder.getPropertiesOf(WeirdTypesClass.class);
		assertThat(properties, hasItem(hasJavaPropertyName("arrayField")));
	}

	@Test
	public void getsListProperty() {
		List<JavaProperty> properties = privateFieldPublicBeanFinder.getPropertiesOf(WeirdTypesClass.class);
		assertThat(properties, hasItem(hasJavaPropertyName("listField")));
	}
	
	@Test
	public void getsVoidProperty() {
		List<JavaProperty> properties = privateFieldPublicBeanFinder.getPropertiesOf(WeirdTypesClass.class);
		assertThat(properties, hasItem(hasJavaPropertyName("voidField")));
	}

	@Test
	public void typeCorrectGivenArrayProperty() {
		List<JavaProperty> properties = privateFieldPublicBeanFinder.getPropertiesOf(WeirdTypesClass.class);
		JavaProperty property = itemOf(properties, that(hasJavaPropertyName("arrayField")));
		assertThat("type", (Object)property.type(), is((Object)String[].class));
	}

	@Test
	public void typeCorrectGivenListProperty() {
		List<JavaProperty> properties = privateFieldPublicBeanFinder.getPropertiesOf(WeirdTypesClass.class);
		JavaProperty property = itemOf(properties, that(hasJavaPropertyName("listField")));
		assertThat("type", (Object)property.type(), is((Object)List.class));
	}
	
	@Test
	public void typeCorrectGivenVoidProperty() {
		List<JavaProperty> properties = privateFieldPublicBeanFinder.getPropertiesOf(WeirdTypesClass.class);
		JavaProperty property = itemOf(properties, that(hasJavaPropertyName("voidField")));
		assertThat("type", (Object)property.type(), is((Object)Void.class));
	}
	
	@Test
	public void typeCorrectGivenBigBBooleanProperty() {
		List<JavaProperty> properties = privateFieldPublicBeanFinder.getPropertiesOf(WeirdTypesClass.class);
		JavaProperty property = itemOf(properties, that(hasJavaPropertyName("bigBBooleanField")));
		assertThat("type", (Object)property.type(), is((Object)Boolean.class));
		assertThat("type", (Object)property.type(), is(not((Object)boolean.class)));
	}

	@Test
	public void typeCorrectGivenLittleBBooleanProperty() {
		List<JavaProperty> properties = privateFieldPublicBeanFinder.getPropertiesOf(WeirdTypesClass.class);
		JavaProperty property = itemOf(properties, that(hasJavaPropertyName("littleBBooleanField")));
		assertThat("type", (Object)property.type(), is((Object)boolean.class));
		assertThat("type", (Object)property.type(), is(not((Object)Boolean.class)));
	}
	
	
	// check avoidance of non-properties
	
	@Test
	public void getsOnlyPropertiesGivenOtherStuff() {
		List<JavaProperty> properties = privateFieldPublicBeanFinder.getPropertiesOf(ThreePropertiesAndOtherStuffClass.class);
		//System.out.println(properties);
		assertThat(properties, containsInAnyOrder(hasJavaPropertyName("property1"),
				hasJavaPropertyName("property2"), hasJavaPropertyName("property3")));
	}
	
	// note: protected/private tests here might not be sufficient because JavaPropertyTest class
	// has direct access anyway
	public static class SimpleIndependentFieldsAndPropertiesClass {
		public String publicField;
		protected String protectedField;
		private String privateField;
		
		public String getPublicProperty() {
			return "hello";
		}
		
		public void setPublicProperty(String value){
		}

		protected String getProtectedProperty() {
			return "hello";
		}
		
		protected void setProtectedProperty(String value){
		}

		private String getPrivateProperty() {
			return "hello";
		}
		
		private void setPrivateProperty(String value){
		}
	}
	
	public static class SimpleStandardBeanClass {
		private String property;
		
		public String getProperty() {
			return property;
		}
		
		public void setProperty(String property) {
			this.property = property;
		}
	}
	
	public static class WeirdTypesClass {
		public String[] arrayField;
		
		public List<String> listField;
		
		public Void voidField;
		
		public boolean littleBBooleanField;
		
		public Boolean bigBBooleanField;
	}
	
	public static class ThreePropertiesAndOtherStuffClass implements Serializable {
		private static final long serialVersionUID = 1L;

		public String property1;
		
		public String getProperty2() {
			return null;
		}
		
		public void setProperty2(String value) {}
		
		private Long property3;
		
		public String calculateName() {
			return ThreePropertiesAndOtherStuffClass.class.getSimpleName();
		}
		
		public static int getDefaultSize() {
			return 3;
		}
	}
}
