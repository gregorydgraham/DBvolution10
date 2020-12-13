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

import java.util.regex.Pattern;

/**
 *
 * @author gregorygraham
 */
public abstract class Regexp {

	private Pattern compiledVersion;

	private Regexp() {
	}

	public abstract String getRegexp();

	public static Regexp startingAnywhere() {
		return new UnescapedSequence("");
	}

	public static Regexp startingFromTheBeginning() {
		return new UnescapedSequence("^");
	}

	public static Regexp or(Regexp regexp, Regexp... regexps) {
		return new Or(regexp, regexps);
	}

	/**
	 * Adds the regular expression to the end of current expression as a new
	 * group.
	 *
	 * <p>
	 * For example Regexp.startingAnywhere().add(allowedValue).add(separator) will
	 * add the "separator" regular expression to the "allowedValue" expression
	 * (the rest of the instruction adds nothing). Assuming that allowedValue is
	 * "[0-9]" and separator is ":", the full regexp will be "([0-9])(:)".
	 *
	 * @param second
	 * @return a new regular expression consisting of the current expression and
	 * the supplied expression added together
	 */
	public Regexp add(Regexp second) {
		return new RegexpCombination(this, second.group());
	}

	/**
	 * Adds the regular expression to the end of current expression without
	 * grouping it.
	 *
	 * <p>
	 * Not grouping the added regular expression can produce counter-intuitive
	 * results and breaks encapsulation so use it carefully. In Particular
	 * extend(myRegex).onceOrNotAtAll() will only apply the "onceOrNotAtAll" to
	 * last element of myRegex and not the entire expression. Using
	 * digit.extend(Regex.startAnywhere().dot().digits()).onceOrNotAtAll() will
	 * match "0." and "0.5" but not "0". If you want grouping use add() instead.
	 *
	 * <p>
	 * For example
	 * Regexp.startingAnywhere().extend(allowedValue).extend(separator) will add
	 * the "separator" regular expression to the "allowedValue" expression (the
	 * rest of the instruction adds nothing). Assuming that allowedValue is
	 * "[0-9]" and separator is ":", the full regexp will be "[0-9]:".
	 *
	 * @param second
	 * @return a new regular expression consisting of the current expression and
	 * the supplied expression added together
	 */
	public Regexp extend(Regexp second) {
		return new RegexpCombination(this, second);
	}

	public Regexp literal(Character character) {
		if (character.equals('/')) {
			return extend(backslash());
		} else {
			return extend(new SingleCharacter(character));
		}
	}

	public Regexp literal(String literals) {
		return extend(new LiteralSequence(literals));
	}

	public Regexp backslash() {
		return extend(new UnescapedSequence("\\\\"));
	}

	public Regexp carat() {
		return extend(new UnescapedSequence("\\^"));
	}

	public Regexp dollarSign() {
		return extend(new UnescapedSequence("\\$"));
	}

	public Regexp dot() {
		return extend(new UnescapedSequence("\\."));
	}

	public Regexp questionMark() {
		return extend(new UnescapedSequence("\\?"));
	}

	public Regexp plus() {
		return extend(new UnescapedSequence("\\+"));
	}

	public Regexp star() {
		return extend(new UnescapedSequence("\\*"));
	}

	public Regexp asterisk() {
		return extend(new UnescapedSequence("\\*"));
	}

	public Regexp pipe() {
		return extend(new UnescapedSequence("\\|"));
	}

	public Regexp squareBracket() {
		return extend(new UnescapedSequence("\\["));
	}

	public Regexp tab() {
		return extend(new UnescapedSequence("\\t"));
	}

	public Regexp newline() {
		return extend(new UnescapedSequence("\\n"));
	}

	public Regexp carriageReturn() {
		return extend(new UnescapedSequence("\\r"));
	}

	public Regexp formfeed() {
		return extend(new UnescapedSequence("\\f"));
	}

	public Regexp bell() {
		return extend(new UnescapedSequence("\\a"));
	}

	public Regexp escapeCharacter() {
		return extend(new UnescapedSequence("\\e"));
	}

	public Regexp controlCharacter(String x) {
		return extend(new UnescapedSequence("\\c" + x));
	}

	public Regexp anyCharacter() {
		return extend(new UnescapedSequence("."));
	}

	public Regexp once() {
		return extend(new UnescapedSequence("{1}"));
	}

	public Regexp thisManyTimes(int x) {
		return extend(new UnescapedSequence("{" + x + "}"));
	}

	public Regexp atLeastThisManyTimes(int x) {
		return extend(new UnescapedSequence("{" + x + ",}"));
	}

	public Regexp atLeastThisXAndNoMoreThanYTimes(int x, int y) {
		return extend(new UnescapedSequence("{" + x + "," + y + "}"));
	}

	public Regexp onceOrNotAtAll() {
		return extend(new UnescapedSequence("?"));
	}

	public Regexp atLeastOnce() {
		return extend(new UnescapedSequence("+"));
	}

	public Regexp oneOrMore() {
		return atLeastOnce();
	}

	public Regexp zeroOrMore() {
		return extend(new UnescapedSequence("*"));
	}

	public Regexp optionalMany() {
		return zeroOrMore();
	}

	public Regexp endOfTheString() {
		return extend(new UnescapedSequence("$"));
	}

	public Regexp digit() {
		return extend(new UnescapedSequence("\\d"));
	}

	public Regexp digits() {
		return digit().oneOrMore();
	}

	public Regexp nondigit() {
		return extend(new UnescapedSequence("\\D"));
	}

	public Regexp nondigits() {
		return nondigit().oneOrMore();
	}

	public Regexp wordCharacter() {
		return extend(new UnescapedSequence("\\w"));
	}

	public Regexp word() {
		return wordCharacter().oneOrMore();
	}

	public Regexp separationBetweenWords() {
		return nonWordCharacter().oneOrMore();
	}

	public Regexp nonWordCharacter() {
		return extend(new UnescapedSequence("\\W"));
	}

	public Regexp whitespace() {
		return extend(new UnescapedSequence("\\w"));
	}

	public Regexp nonWhitespace() {
		return extend(new UnescapedSequence("\\W"));
	}

	public Regexp space() {
		return extend(new UnescapedSequence(" "));
	}

	public Regexp capture(Regexp regexp) {
		return new UnescapedSequence("(" + regexp.getRegexp() + ")");
	}

	public Regexp negativeInteger() {
		return extend(literal('-').anyBetween('1', '9').once().digit().zeroOrMore());
	}

	public Regexp positiveInteger() {
		return extend(POSITIVE_INTEGER_PATTERN);
	}
	protected static final Regexp POSITIVE_INTEGER_PATTERN = startingAnywhere().notPrecededBy("-").plus().onceOrNotAtAll().digit().atLeastOnce();

	public Regexp integer() {
		return extend(Regexp.or(new UnescapedSequence("-"), new UnescapedSequence("+")).onceOrNotAtAll().anyBetween('1', '9').once().digit().zeroOrMore());
	}

	public Regexp realNumber() {
		return extend(Regexp.or(
				new LiteralSequence("-"),
				new LiteralSequence("+")
		).onceOrNotAtAll()
				.digit().atLeastOnce()
				.extend(startingAnywhere()
								.dot().once()
								.digit().oneOrMore()
				).onceOrNotAtAll()
		);
	}

	public Regexp anyBetween(Character lowest, Character highest) {
		return extend(new Range(lowest, highest));
	}

	public Regexp anyOf(String literals) {
		return extend(new Range(literals));
	}

	public Regexp.Group group() {
		return new Group(this);
	}

	protected final Pattern getCompiledVersion() {
		if (compiledVersion == null) {
			final String regexp = this.getRegexp();
			System.out.println("nz.co.gregs.dbvolution.utility.Regexp.matchesEntireString(): " + regexp);
			compiledVersion = Pattern.compile(regexp);
		}
		return compiledVersion;
	}

	public boolean matchesEntireString(String string) {
		return getCompiledVersion().matcher(string).matches();
	}

	public boolean matchesWithinString(String string) {
		return getCompiledVersion().matcher(string).find();
	}

	public Regexp notPrecededBy(String string) {
		return new UnescapedSequence("(?<!")
				.extend(new LiteralSequence(string))
				.extend(new UnescapedSequence(")"));
	}

	public static class SingleCharacter extends Regexp {

		private final Character literal;

		protected SingleCharacter(Character character) {
			this.literal = character;
		}

		@Override
		public String getRegexp() {
			return "" + literal;
		}
	}

	public static class LiteralSequence extends Regexp {

		private final String literal;

		public LiteralSequence(String literals) {
			if (literals == null) {
				this.literal = "";
			} else {
				this.literal = literals
						.replaceAll("\\\\", "\\")
						.replaceAll("\\.", "\\.")
						.replaceAll("\\?", "\\?")
						.replaceAll("\\+", "\\+")
						.replaceAll("\\*", "\\*")
						.replaceAll("\\^", "\\^")
						.replaceAll("\\$", "\\$")
						.replaceAll("\\|", "\\|")
						.replaceAll("\\[", "\\[");
			}
		}

		@Override
		public String getRegexp() {
			return "" + literal;
		}
	}

	public static class Range extends Regexp {

		private final String literals;

		public Range(Character lowest, Character highest) {
			this.literals = lowest + "-" + highest;
		}

		public Range(String literals) {
			this.literals = literals;
		}

		public Regexp not() {
			return new NegatedCharacterRange(this);
		}

		public Regexp negated() {
			return new NegatedCharacterRange(this);
		}

		public Range includeMinus() {
			return new Range("-" + this.literals);
		}

		public Range and(Character lowest, Character highest) {
			return and(new Range(lowest, highest));
		}

		public Range and(String literals) {
			return and(new Range(literals));
		}

		public Range and(Range range) {
			return new Range(this.literals + range.getRange());
		}

		public Range excluding(Range range) {
			return new Range(this.literals + "-[" + range.getRange() + "]");
		}

		@Override
		public String getRegexp() {
			return "[" + getRange() + "]";
		}

		public String getRange() {
			return literals;
		}
	}

	protected static class UnescapedSequence extends Regexp {

		private final String literal;

		protected UnescapedSequence(String literals) {
			if (literals == null) {
				this.literal = "";
			} else {
				this.literal = literals;
			}
		}

		@Override
		public String getRegexp() {
			return "" + literal;
		}
	}

	protected static class RegexpCombination extends Regexp {

		private final Regexp first;
		private final Regexp second;

		protected RegexpCombination(Regexp first, Regexp second) {
			this.first = first;
			this.second = second;
		}

		@Override
		public String getRegexp() {
			return first.getRegexp() + second.getRegexp();
		}
	}

	public static class NegatedCharacterRange extends Regexp {

		private final Range range;

		public NegatedCharacterRange(Range range) {
			this.range = range;
		}

		@Override
		public String getRegexp() {
			return "[^" + range.getRange() + "]";
		}

	}

	private static class Or extends Regexp {

		private final SeparatedString sepString;

		public Or(Regexp first, Regexp... regexps) {
			sepString = SeparatedString
					.forSeparator("|")
					.withWrapBefore("(")
					.withWrapAfter(")")
					.add(first.getRegexp())
					.addAll(
							(t) -> {
								return t.getRegexp();
							}, regexps);
		}

		@Override
		public String getRegexp() {
			return sepString.toString();
		}
	}

	public static class Group extends Regexp {

		private final Regexp regexp;

		public Group(Regexp regexp) {
			this.regexp = regexp;
		}

		@Override
		public String getRegexp() {
			return "(" + regexp.getRegexp() + ")";
		}
	}

}
