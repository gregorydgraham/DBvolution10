/*
 * Copyright 2019 Gregory Graham.
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
package nz.co.gregs.dbvolution.utility;

import java.security.SecureRandom;

/**
 *
 * @author gregorygraham
 */
public class Random {

	private static final String CHAR_LOWER = "abcdefghijklmnopqrstuvwxyz";
	private static final String CHAR_UPPER = CHAR_LOWER.toUpperCase();
	private static final String NUMBER = "0123456789";

	private static final String DATA_FOR_RANDOM_STRING = CHAR_LOWER + CHAR_UPPER + NUMBER;
	private static final SecureRandom RANDOM = new SecureRandom();

	private Random() {
	}
	
	public static synchronized int number(){
		return RANDOM.nextInt();
	}
	
	public static synchronized int NumberBetween(int lower, int upper){
		// between 1-6 -> 5 but nextInt(5) never returns 5 so +1 -> 6
		int diff = upper-lower;
		int randomNumber = RANDOM.nextInt(diff+1);
		int adjustedNumber = randomNumber+lower;
		return adjustedNumber;
	}

	public static synchronized byte[] bytes(int i) {
		byte[] bytes = new byte[i];
		RANDOM.nextBytes(bytes);
		return bytes;

	}

	public static synchronized String string(int length) {
		if (length < 1) {
			return "";
		}

		StringBuilder sb = new StringBuilder(length);
		for (int i = 0; i < length; i++) {

			// 0-62 (exclusive), random returns 0-61
			int rndCharAt = RANDOM.nextInt(DATA_FOR_RANDOM_STRING.length());
			char rndChar = DATA_FOR_RANDOM_STRING.charAt(rndCharAt);
			
			sb.append(rndChar);
		}
		return sb.toString();
	}
}
