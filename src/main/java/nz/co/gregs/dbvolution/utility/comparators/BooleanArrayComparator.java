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

/**
 *
 * @author gregorygraham
 */
public class BooleanArrayComparator implements Comparator<Boolean[]>, Serializable {

	private static final long serialVersionUID = 1L;

	public BooleanArrayComparator() {
	}

	@Override
	public int compare(Boolean[] o1, Boolean[] o2) {
		if (o1.length < o2.length) {
			return -1;
		} else if (o1.length > o2.length) {
			return 1;
		} else {
			for (int i = 0; i < o1.length; i++) {
				final int compareTo = o1[i].compareTo(o2[i]);
				if (compareTo != 0) {
					return compareTo;
				}
			}
			return 0;
		}
	}

}
