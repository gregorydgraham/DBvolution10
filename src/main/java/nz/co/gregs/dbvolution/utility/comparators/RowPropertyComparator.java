/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package nz.co.gregs.dbvolution.utility.comparators;

import java.io.Serializable;
import java.util.Comparator;
import nz.co.gregs.dbvolution.DBRow;
import nz.co.gregs.dbvolution.datatypes.QueryableDatatype;
import nz.co.gregs.dbvolution.internal.properties.PropertyWrapper;

/**
 *
 * @author gregorygraham
 * @param <R> the DBRow type this comparator works on
 */
public class RowPropertyComparator<R extends DBRow> implements Comparator<R>, Serializable {

	private static final long serialVersionUID = 1L;
	
	private final PropertyWrapper<?, ?, ?> prop;

	public RowPropertyComparator(R row, QueryableDatatype<?> column) {
		this.prop = row.getPropertyWrapperOf(column);
	}

	@Override
	public int compare(R o1, R o2) {
		return prop.compareBetweenRows(o1, o2);
	}
	
}
