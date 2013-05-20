/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package nz.co.gregs.dbvolution;

import java.beans.BeanInfo;
import java.beans.IntrospectionException;
import java.beans.Introspector;
import java.beans.PropertyDescriptor;
import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.logging.Level;
import java.util.logging.Logger;
import nz.co.gregs.dbvolution.annotations.DBTableColumn;
import nz.co.gregs.dbvolution.annotations.DBTableName;
import nz.co.gregs.dbvolution.annotations.DBTablePrimaryKey;

/**
 *
 * @author gregory.graham
 */
abstract public class DBTableRow {

    public DBTableRow() {
    }

    public String getPrimaryKey() throws IntrospectionException, IllegalAccessException, IllegalArgumentException, InvocationTargetException {
        String pkColumnValue = "";
        @SuppressWarnings("unchecked")
        Class<? extends DBTableRow> thisClass = this.getClass();
        Field[] fields = thisClass.getDeclaredFields();
        for (Field field : fields) {
            if (field.isAnnotationPresent(DBTablePrimaryKey.class)) {
                pkColumnValue = this.getQueryableValueOfField(field).toString();
            }
        }
        if (pkColumnValue.isEmpty()) {
            throw new RuntimeException("Primary Key Field Not Defined: Please define the primay key field using the @DBTablePrimaryKey annotation.");
        } else {
            return pkColumnValue;
        }

    }

    /**
     * DO NOT USE THIS.
     *
     * @param <Q>
     * @param field
     * @return
     * @throws IntrospectionException
     * @throws IllegalArgumentException
     * @throws InvocationTargetException
     */
    @SuppressWarnings("unchecked")
    public <Q extends QueryableDatatype> Q getQueryableValueOfField(Field field) throws IntrospectionException, IllegalArgumentException, InvocationTargetException {
        BeanInfo info = Introspector.getBeanInfo(this.getClass());
        PropertyDescriptor[] descriptors = info.getPropertyDescriptors();
        for (PropertyDescriptor pd : descriptors) {
            if (pd.getName().equals(field.getName())) {
                Method readMethod = pd.getReadMethod();
                if (readMethod != null) {
                    try {
                        return (Q) readMethod.invoke(this);
                    } catch (IllegalAccessException ex) {
                        throw new RuntimeException("GET Method Found But Unable To Access: Please change GET method to public for " + this.getClass().getSimpleName() + "." + field.getName(), ex);
                    }
                }
            }
        }
        try {
            // no GET method found so try direct method
            return (Q) field.get(this);
            //throw new UnsupportedOperationException("No Appropriate Get Method Found In " + this.getClass().getSimpleName() + " for " + field.toGenericString());
        } catch (IllegalAccessException ex) {
            throw new RuntimeException("Unable To Access Variable Nor GET Method: Please change protection to public for GET method or field " + this.getClass().getSimpleName() + "." + field.getName(), ex);
        }
    }
    
    /**     
     * Probably not needed by the programmer, this is the convenience function
     * to find the table name specified by
     *
     * @DBTableName
     *
     * @return the name of the table in the database specified to correlate with
     * the specified type
     * 
     */
    public String getTableName() {
        @SuppressWarnings("unchecked")
        Class<? extends DBTableRow> thisClass = (Class<? extends DBTableRow>) this.getClass();
        if (thisClass.isAnnotationPresent(DBTableName.class)) {
            DBTableName annotation = thisClass.getAnnotation(DBTableName.class);
            return annotation.value();
        } else {
            return thisClass.getSimpleName();
        }
    }
    

    
    @Override
    public String toString() {
        StringBuilder string = new StringBuilder();
        Class<? extends DBTableRow> thisClass = this.getClass();
        Field[] fields = thisClass.getDeclaredFields();

        String separator = "";

        for (Field field : fields) {
            if (field.isAnnotationPresent(DBTableColumn.class)) {
                try {
                    string.append(separator);
                    string.append(" ");
                    string.append(field.getName());
                    string.append(":");
                    try {
                        string.append(getQueryableValueOfField(field));
                    } catch (IntrospectionException ex) {
                        Logger.getLogger(DBTableRow.class.getName()).log(Level.SEVERE, null, ex);
                    } catch (InvocationTargetException ex) {
                        Logger.getLogger(DBTableRow.class.getName()).log(Level.SEVERE, null, ex);
                    }
                } catch (IllegalArgumentException ex) {
                    Logger.getLogger(DBTableRow.class.getName()).log(Level.SEVERE, null, ex);
                }
                separator = ",";
            }
        }
        return string.toString();
    }
}
