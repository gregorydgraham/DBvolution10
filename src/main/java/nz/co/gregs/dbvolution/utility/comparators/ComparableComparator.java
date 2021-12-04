/*
 * Copyright 2020 Gregory Graham.
 *
 * Commercial licenses are available, please contact info@gregs.co.nz for details.
 * 
 * This work is licensed under the Creative Commons Attribution-NonCommercial-ShareAlike 4.0 International License. 
 * To view a copy of this license, visit http://creativecommons.org/licenses/by-nc-sa/4.0/ 
 * or send a letter to Creative Commons, PO Box 1866, Mountain View, CA 94042, USA.
 * 
 * You are free to:
 *     Share - copy and redistribute the material in any medium or format
 *     Adapt - remix, transform, and build upon the material
 * 
 *     The licensor cannot revoke these freedoms as long as you follow the license terms.               
 *     Under the following terms:
 *                 
 *         Attribution - 
 *             You must give appropriate credit, provide a link to the license, and indicate if changes were made. 
 *             You may do so in any reasonable manner, but not in any way that suggests the licensor endorses you or your use.
 *         NonCommercial - 
 *             You may not use the material for commercial purposes.
 *         ShareAlike - 
 *             If you remix, transform, or build upon the material, 
 *             you must distribute your contributions under the same license as the original.
 *         No additional restrictions - 
 *             You may not apply legal terms or technological measures that legally restrict others from doing anything the 
 *             license permits.
 * 
 * Check the Creative Commons website for any details, legalese, and updates.
 */
package nz.co.gregs.dbvolution.utility.comparators;

import java.io.Serializable;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Map;

/**
 *
 * @author gregorygraham
 * @param <T> The class for which this comparator works
 */
public class ComparableComparator<T extends Comparable<? super T>> implements Comparator<T>, Serializable {

	private static final long serialVersionUID = 1L;

	private static final Map<Class<?>, ComparableComparator<?>> map = new HashMap<>(0);

	public static <C extends Comparable<? super C>> ComparableComparator<C> forClass(Class<C> aClass) {
		@SuppressWarnings("unchecked")
		ComparableComparator<C> got = (ComparableComparator<C>) map.get(aClass);
		if (got == null) {
			got = new ComparableComparator<C>(aClass);
			map.put(aClass, got);
		}
		return got;
	}

	private ComparableComparator() {
		super();
	}

	private ComparableComparator(Class<T> aClass) {
		this();
	}

	@Override
	public int compare(T o1, T o2) {
		if (o1 != null) {
			return o1.compareTo(o2);
		} else {
			return o2 == null ? 0 : 1;
		}
	}

}
