package nz.co.gregs.dbvolution.internal;

import static nz.co.gregs.dbvolution.internal.PropertyMatchers.*;
import static org.hamcrest.Matchers.*;
import static org.junit.Assert.*;

import java.io.Serializable;
import java.util.List;

import nz.co.gregs.dbvolution.annotations.DBAdaptType;
import nz.co.gregs.dbvolution.annotations.DBColumn;
import nz.co.gregs.dbvolution.annotations.DBPrimaryKey;
import nz.co.gregs.dbvolution.datatypes.DBInteger;
import nz.co.gregs.dbvolution.datatypes.DBString;
import nz.co.gregs.dbvolution.datatypes.DBTypeAdaptor;
import nz.co.gregs.dbvolution.exceptions.DBPebkacException;
import nz.co.gregs.dbvolution.exceptions.DBThrownByEndUserCodeException;
import nz.co.gregs.dbvolution.internal.JavaPropertyFinder.PropertyType;
import nz.co.gregs.dbvolution.internal.JavaPropertyFinder.Visibility;

import org.junit.Test;

@SuppressWarnings("unused")
public class JavaPropertyTest {

    private JavaPropertyFinder privateFieldPublicBeanFinder = new JavaPropertyFinder(
            Visibility.PRIVATE, Visibility.PUBLIC, null, (PropertyType[]) null);

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
    @Test(expected = UnsupportedOperationException.class)
    public void errorsTryingToGetPrivateProperties() {
        JavaPropertyFinder finder = new JavaPropertyFinder(
                Visibility.PRIVATE, Visibility.PRIVATE, null, (PropertyType[]) null);
        finder.getPropertiesOf(SimpleIndependentFieldsAndPropertiesClass.class);
    }

    // check shadowing effects
    @Test
    public void getsShadowingPrivateFieldGivenStandardBean() {
        List<JavaProperty> properties = privateFieldPublicBeanFinder.getPropertiesOf(SimpleStandardBeanClass.class);
        // will contain two 'property' properties, one is java field
        assertThat(properties, hasItem(allOf(hasJavaPropertyName("property"), isJavaPropertyField())));
    }

    @Test
    public void getsShadowingPublicPropertyGivenStandardBean() {
        List<JavaProperty> properties = privateFieldPublicBeanFinder.getPropertiesOf(SimpleStandardBeanClass.class);
        // will contain two 'property' properties, one is java bean-property
        assertThat(properties, hasItem(allOf(hasJavaPropertyName("property"), not(isJavaPropertyField()))));
    }

    // check visibility control
    @Test
    public void getsPublicFieldGivenPublicOnlyFieldVisibility() {
        JavaPropertyFinder finder = new JavaPropertyFinder(
                Visibility.PUBLIC, Visibility.PUBLIC, null, (PropertyType[]) null);

        List<JavaProperty> properties = finder.getPropertiesOf(SimpleIndependentFieldsAndPropertiesClass.class);
        assertThat(properties, hasItem(hasJavaPropertyName("publicField")));
    }

    @Test
    public void getsProtectedFieldGivenProtectedFieldVisibility() {
        JavaPropertyFinder finder = new JavaPropertyFinder(
                Visibility.PROTECTED, Visibility.PUBLIC, null, (PropertyType[]) null);

        List<JavaProperty> properties = finder.getPropertiesOf(SimpleIndependentFieldsAndPropertiesClass.class);
        assertThat(properties, hasItem(hasJavaPropertyName("protectedField")));
    }

    @Test
    public void getsProtectedFieldGivenDefaultFieldVisibility() {
        JavaPropertyFinder finder = new JavaPropertyFinder(
                Visibility.DEFAULT, Visibility.PUBLIC, null, (PropertyType[]) null);

        List<JavaProperty> properties = finder.getPropertiesOf(SimpleIndependentFieldsAndPropertiesClass.class);
        assertThat(properties, hasItem(hasJavaPropertyName("protectedField")));
    }

    @Test
    public void cantGetProtectedFieldGivenPublicOnlyFieldVisibility() {
        JavaPropertyFinder finder = new JavaPropertyFinder(
                Visibility.PUBLIC, Visibility.PUBLIC, null, (PropertyType[]) null);

        List<JavaProperty> properties = finder.getPropertiesOf(SimpleIndependentFieldsAndPropertiesClass.class);
        assertThat(properties, not(hasItem(hasJavaPropertyName("privateField"))));
    }

    @Test
    public void cantGetPrivateFieldGivenPublicOnlyFieldVisibility() {
        JavaPropertyFinder finder = new JavaPropertyFinder(
                Visibility.PUBLIC, Visibility.PUBLIC, null, (PropertyType[]) null);

        List<JavaProperty> properties = finder.getPropertiesOf(SimpleIndependentFieldsAndPropertiesClass.class);
        assertThat(properties, not(hasItem(hasJavaPropertyName("privateField"))));
    }

    @Test
    public void cantGetPrivateFieldGivenProtectedFieldVisibility() {
        JavaPropertyFinder finder = new JavaPropertyFinder(
                Visibility.PROTECTED, Visibility.PUBLIC, null, (PropertyType[]) null);

        List<JavaProperty> properties = finder.getPropertiesOf(SimpleIndependentFieldsAndPropertiesClass.class);
        assertThat(properties, not(hasItem(hasJavaPropertyName("privateField"))));
    }

    @Test
    public void cantGetPrivateFieldGivenDefaultFieldVisibility() {
        JavaPropertyFinder finder = new JavaPropertyFinder(
                Visibility.DEFAULT, Visibility.PUBLIC, null, (PropertyType[]) null);

        List<JavaProperty> properties = finder.getPropertiesOf(SimpleIndependentFieldsAndPropertiesClass.class);
        assertThat(properties, not(hasItem(hasJavaPropertyName("privateField"))));
    }

    // check avoidance of non-properties
    
    @Test
    @SuppressWarnings("unchecked")
    public void getsOnlyPropertiesGivenOtherStuff() {
        List<JavaProperty> properties = privateFieldPublicBeanFinder.getPropertiesOf(ThreePropertiesAndOtherStuffClass.class);
        //System.out.println(properties);
        assertThat(properties, containsInAnyOrder(hasJavaPropertyName("property1"),
                hasJavaPropertyName("property2"), hasJavaPropertyName("property3")));
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
        assertThat("type", (Object) property.type(), is((Object) String[].class));
    }

    @Test
    public void typeCorrectGivenListProperty() {
        List<JavaProperty> properties = privateFieldPublicBeanFinder.getPropertiesOf(WeirdTypesClass.class);
        JavaProperty property = itemOf(properties, that(hasJavaPropertyName("listField")));
        assertThat("type", (Object) property.type(), is((Object) List.class));
    }

    @Test
    public void typeCorrectGivenVoidProperty() {
        List<JavaProperty> properties = privateFieldPublicBeanFinder.getPropertiesOf(WeirdTypesClass.class);
        JavaProperty property = itemOf(properties, that(hasJavaPropertyName("voidField")));
        assertThat("type", (Object) property.type(), is((Object) Void.class));
    }

    @Test
    public void typeCorrectGivenBigBBooleanProperty() {
        List<JavaProperty> properties = privateFieldPublicBeanFinder.getPropertiesOf(WeirdTypesClass.class);
        JavaProperty property = itemOf(properties, that(hasJavaPropertyName("bigBBooleanField")));
        assertThat("type", (Object) property.type(), is((Object) Boolean.class));
        assertThat("type", (Object) property.type(), is(not((Object) boolean.class)));
    }

    @Test
    public void typeCorrectGivenLittleBBooleanProperty() {
        List<JavaProperty> properties = privateFieldPublicBeanFinder.getPropertiesOf(WeirdTypesClass.class);
        JavaProperty property = itemOf(properties, that(hasJavaPropertyName("littleBBooleanField")));
        assertThat("type", (Object) property.type(), is((Object) boolean.class));
        assertThat("type", (Object) property.type(), is(not((Object) Boolean.class)));
    }

    // check access to property values
    
    @Test
    public void readsPublicField() {
    	SimpleIndependentFieldsAndPropertiesClass obj = new SimpleIndependentFieldsAndPropertiesClass();
    	obj.publicField = "hello"; 
    	JavaProperty property = propertyOf(obj, "publicField");
        assertThat((String)property.get(obj), is("hello"));
    }

    @Test
    public void writesPublicField() {
    	SimpleIndependentFieldsAndPropertiesClass obj = new SimpleIndependentFieldsAndPropertiesClass();
    	JavaProperty property = propertyOf(obj, "publicField");
    	property.set(obj, "hello");
        assertThat(obj.publicField, is("hello"));
    }

    @Test
    public void readsPrivateField() {
    	SimpleIndependentFieldsAndPropertiesClass obj = new SimpleIndependentFieldsAndPropertiesClass();
    	obj.privateField = "hello"; 
    	JavaProperty property = propertyOf(obj, "privateField");
        assertThat((String)property.get(obj), is("hello"));
    }

    @Test
    public void writesPrivateField() {
    	SimpleIndependentFieldsAndPropertiesClass obj = new SimpleIndependentFieldsAndPropertiesClass();
    	JavaProperty property = propertyOf(obj, "privateField");
    	property.set(obj, "hello");
        assertThat(obj.privateField, is("hello"));
    }
    
    @Test
    public void readsPublicBeanProperty() {
    	SimpleIndependentFieldsAndPropertiesClass obj = new SimpleIndependentFieldsAndPropertiesClass();
    	obj.setPublicProperty("hello");
    	JavaProperty property = propertyOf(obj, "publicProperty");
        assertThat((String)property.get(obj), is("hello"));
    }

    @Test
    public void writesPublicBeanProperty() {
    	SimpleIndependentFieldsAndPropertiesClass obj = new SimpleIndependentFieldsAndPropertiesClass();
    	JavaProperty property = propertyOf(obj, "publicProperty");
    	property.set(obj, "hello");
        assertThat(obj.getPublicProperty(), is("hello"));
    }
    
    @Test(expected=DBThrownByEndUserCodeException.class)
    public void handlesUserExceptionWhenReadingBeanProperty() {
        class TestClass {
			public int getProperty() {
                throw new ArrayIndexOutOfBoundsException();
            }
        }

        JavaProperty property = propertyOf(TestClass.class, "property");
        property.get(new TestClass());
    }

    @Test(expected=DBThrownByEndUserCodeException.class)
    public void handlesUserExceptionWhenWritingBeanProperty() {
        class TestClass {
			public void setProperty(int value) {
                throw new ArrayIndexOutOfBoundsException("bar");
            }
        }
        JavaProperty property = propertyOf(TestClass.class, "property");
        property.set(new TestClass(), 23);
    }
    
    // check handling of property types (including inconsistencies)
    
    @Test
    public void getsPrimitiveTypeGivenPrimitive() {
        class TestClass {
			public int getProperty() {
				return 0;
			}
			
			public void setProperty(int value) {
            }
        }

        JavaProperty property = propertyOf(TestClass.class, "property");
        assertThat(property.type(), is((Object)int.class));
    }

    @Test
    public void getsNumberWrapperTypeGivenNumberWrapper() {
        class TestClass {
			public Integer getProperty() {
				return 0;
			}
			
			public void setProperty(Integer value) {
            }
        }

        JavaProperty property = propertyOf(TestClass.class, "property");
        assertThat(property.type(), is((Object)Integer.class));
    }

//  // Turns out java.beans.PropertyDescriptor only returns the getter
//  // if the types are different, and the setter just gets dropped
//    @Test
//    public void getsMostConcreteTypeGivenSetterIsSupertypeOfGetter() {
//        class TestClass {
//			public Integer getProperty() {
//				return 0;
//			}
//			
//			public void setProperty(Number value) {
//            }
//        }
//
//        // assert this test is sane
//        TestClass obj = new TestClass();
//        Integer valueAsInteger = obj.getProperty();
//        Number valueAsNumber = obj.getProperty();
//        obj.setProperty(valueAsInteger);
//        obj.setProperty(valueAsNumber);
//
//        // assert implementation is correct
//        JavaProperty property = propertyOf(TestClass.class, "property");
//        assertThat(property.type(), is((Object)Number.class));
//    }
    
//    // Turns out java.beans.PropertyDescriptor only returns the getter
//    // if the types are different, and the setter just gets dropped
//    @Test(expected=DBPebkacException.class)
//    public void errorsWhenRetrievingTypeGivenGetterIsSupertypeOfSetter() {
//        class TestClass {
//			public Number getProperty() {
//				return 0;
//			}
//			
//			public void setProperty(Integer value) {
//            }
//        }
//        
//        // assert this test is sane
//        TestClass obj = new TestClass();
//        Integer valueAsInteger = null;
//        Number valueAsNumber = null;
//        //valueAsInteger = obj.getProperty(); // won't compile
//        valueAsNumber = obj.getProperty(); // can't write using this type
//        obj.setProperty(valueAsInteger); // can't read using this type
//        //obj.setProperty(valueAsNumber); // won't compile
//
//        // assert implementation is correct
//        JavaProperty property = propertyOf(TestClass.class, "property");
//        property.type();
//    }

//  // Turns out java.beans.PropertyDescriptor only returns the getter
//  // if the types are different, and the setter just gets dropped
//    @Test(expected=DBPebkacException.class)
//    public void errorsWhenRetrievingTypeGivenInconsistentOnAccessorMethods() {
//        class TestClass {
//			public int getProperty() {
//				return 0;
//			}
//			
//			public void setProperty(String value) {
//            }
//        }
//
//        List<JavaProperty> properties = privateFieldPublicBeanFinder.getPropertiesOf(TestClass.class);
//        for (JavaProperty prop: properties) {
//        	System.out.println(prop);
//        }
//        
//        JavaProperty property = propertyOf(TestClass.class, "property");
//        property.type();
//    }
    
    // check handling of annotations (including duplicates)
    
    @Test
    public void retrievesAnnotationGivenExactlyDuplicatedAnnotationOnGetterAndSetter() {
        class TestClass {

            @DBColumn("samename")
            public int getProperty() {
                return 0;
            }

            @DBColumn("samename")
            public void setProperty(int value) {
            }
        }

        JavaProperty property = propertyOf(TestClass.class, "property");
        assertThat(property.getAnnotation(DBColumn.class), is(not(nullValue())));
    }

    @Test
    public void retrievesAnnotationGivenExactlyDuplicatedEmptyAnnotationOnGetterAndSetter() {
        class TestClass {

            @DBColumn
            public int getProperty() {
                return 0;
            }

            @DBColumn
            public void setProperty(int value) {
            }
        }

        JavaProperty property = propertyOf(TestClass.class, "property");
        assertThat(property.getAnnotation(DBColumn.class), is(not(nullValue())));
    }

    @Test(expected = DBPebkacException.class)
    public void errorsWhenRetrievingAnnotationGivenDifferentDuplicatedSimpleAnnotationOnGetterAndSetter() {
        class TestClass {

            @DBColumn("samename")
            public int getProperty() {
                return 0;
            }

            @DBColumn("differentname")
            public void setProperty(int value) {
            }
        }

        JavaProperty property = propertyOf(TestClass.class, "property");
        property.getAnnotation(DBColumn.class);
    }

    @Test
    public void acceptsAnnotationWhenRetrievingAnnotationGivenSemanticallyIdenticalAnnotationOnGetterAndSetter() {
        class TestClass {

            @DBColumn("")
            public int getProperty() {
                return 0;
            }

            @DBColumn
            public void setProperty(int value) {
            }
        }

        JavaProperty property = propertyOf(TestClass.class, "property");
        assertThat(property.getAnnotation(DBColumn.class), is(not(nullValue())));
    }
    
    @Test(expected = DBPebkacException.class)
    public void errorsWhenRetrievingAnnotationGivenDifferentDuplicatedComplexAnnotationOnGetterAndSetter() {
    	class MyAdaptor implements DBTypeAdaptor<Object, DBInteger> {
			public Object fromDatabaseValue(DBInteger dbvValue) {
				return null;
			}
			public DBInteger toDatabaseValue(Object objectValue) {
				return null;
			}
    	}
    	
        class TestClass {
            @DBAdaptType(adaptor=MyAdaptor.class, type=DBString.class)
            public int getProperty() {
                return 0;
            }

            @DBAdaptType(adaptor=MyAdaptor.class, type=DBInteger.class)
            public void setProperty(int value) {
            }
        }

        JavaProperty property = propertyOf(TestClass.class, "property");
        property.getAnnotation(DBAdaptType.class);
    }

    @Test(expected = DBPebkacException.class)
    public void errorsWhenRetrievingAnnotationGivenDifferentDuplicatedDefaultedComplexAnnotationOnGetterAndSetter() {
    	class MyAdaptor implements DBTypeAdaptor<Object, DBInteger> {
			public Object fromDatabaseValue(DBInteger dbvValue) {
				return null;
			}
			public DBInteger toDatabaseValue(Object objectValue) {
				return null;
			}
    	}
    	
        class TestClass {
            @DBAdaptType(adaptor=MyAdaptor.class)
            public int getProperty() {
                return 0;
            }

            @DBAdaptType(adaptor=MyAdaptor.class, type=DBInteger.class)
            public void setProperty(int value) {
            }
        }

        JavaProperty property = propertyOf(TestClass.class, "property");
        property.getAnnotation(DBAdaptType.class);
    }
    
    @Test
    public void retrievesAnnotationsGivenAnnotationsOnAlternatingGetterOrSetter() {
        class TestClass {

            @DBColumn("name")
            public int getProperty() {
                return 0;
            }

            @DBPrimaryKey
            public void setProperty(int value) {
            }
        }

        JavaProperty property = propertyOf(TestClass.class, "property");
        assertThat(property.getAnnotation(DBColumn.class), is(not(nullValue())));
        assertThat(property.getAnnotation(DBPrimaryKey.class), is(not(nullValue())));
    }

    private JavaProperty propertyOf(Object obj, String javaPropertyName) {
    	return propertyOf(obj.getClass(), javaPropertyName);
    }
    
    private JavaProperty propertyOf(Class<?> clazz, String javaPropertyName) {
        List<JavaProperty> properties = privateFieldPublicBeanFinder.getPropertiesOf(clazz);
        JavaProperty property = itemOf(properties, that(hasJavaPropertyName(javaPropertyName)));
        if (property == null) {
            throw new IllegalArgumentException("No property found with java name '" + javaPropertyName + "'");
        }
        return property;
    }

    // note: protected/private tests here might not be sufficient because JavaPropertyTest class
    // has direct access anyway
    public static class SimpleIndependentFieldsAndPropertiesClass {

        public String publicField;
        protected String protectedField;
        private String privateField;
        
        private String _publicProperty;
        private String _protectedProperty;

        public String getPublicProperty() {
            return _publicProperty;
        }

        public void setPublicProperty(String value) {
        	this._publicProperty = value;
        }

        protected String getProtectedProperty() {
            return _protectedProperty;
        }

        protected void setProtectedProperty(String value) {
        	this._protectedProperty = value;
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

    @SuppressWarnings("unused")
    public static class ThreePropertiesAndOtherStuffClass implements Serializable {
        private static final long serialVersionUID = 1L;
        public String property1;

        public String getProperty2() {
            return null;
        }

        public void setProperty2(String value) {
        }
		private Long property3;

        public String calculateName() {
            return ThreePropertiesAndOtherStuffClass.class.getSimpleName();
        }

        public static int getDefaultSize() {
            return 3;
        }
    }
}
