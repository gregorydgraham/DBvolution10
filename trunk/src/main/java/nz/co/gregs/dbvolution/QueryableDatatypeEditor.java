/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package nz.co.gregs.dbvolution;

import java.beans.PropertyEditorSupport;

/**
 *
 * @author gregory.graham
 */
public class QueryableDatatypeEditor extends PropertyEditorSupport {

    private String format;

    public void setFormat(String format) {
        this.format = format;
    }

    @Override
    public void setAsText(String text) {
        if (format != null && format.equals("upperCase")) {
            text = text.toUpperCase();
        }
        Object value = getValue();
        if (value instanceof QueryableDatatype) {
            QueryableDatatype qdt = (QueryableDatatype) value;
            qdt.isLiterally(text);
        } else {
            QueryableDatatype type = new QueryableDatatype();
            type.isLiterally(text);
            setValue(type);
        }
    }
}