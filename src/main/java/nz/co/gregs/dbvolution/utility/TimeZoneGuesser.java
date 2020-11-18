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
package nz.co.gregs.dbvolution.utility;

import java.time.Duration;
import java.time.LocalDateTime;

/**
 *
 * @author gregorygraham
 */
public class TimeZoneGuesser {

	public static TimeZoneGuesser guess(LocalDateTime systemLocalDatetime) {

		final LocalDateTime now = LocalDateTime.now();
		Duration difference = Duration.between(systemLocalDatetime, now);
		if (difference.abs().compareTo(Duration.ofMinutes(20)) <= 0) {
			return new TimeZoneGuesser();
		} else {
			long toMinutes = difference.toMinutes();
			toMinutes = toMinutes - (toMinutes % 15);
			Duration roundedDiff = Duration.ofMinutes(toMinutes);
			int sign = roundedDiff.toHoursPart() / difference.abs().toHoursPart();

			int hours = roundedDiff.toHoursPart();
			int minutes = roundedDiff.toMinutesPart();
			return new TimeZoneGuesser(hours, minutes);
		}
	}

	private final int hours;
	private final int minutes;

	private TimeZoneGuesser() {
		this.hours = 0;
		this.minutes = 0;
	}

	private TimeZoneGuesser(int hours, int minutes) {
		this.hours = hours;
		this.minutes = minutes;
	}

	public int getHours() {
		return hours;
	}

	public int getMinutes() {
		return minutes;
	}

}
