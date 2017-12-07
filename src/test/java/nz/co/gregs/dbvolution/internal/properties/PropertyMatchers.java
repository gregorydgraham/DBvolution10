package nz.co.gregs.dbvolution.internal.properties;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import org.hamcrest.BaseMatcher;
import org.hamcrest.Description;
import org.hamcrest.Matcher;
import org.hamcrest.StringDescription;
import org.hamcrest.TypeSafeDiagnosingMatcher;
import org.hamcrest.TypeSafeMatcher;

class PropertyMatchers {

	public static Matcher<String> matchesRegex(final String regex) {
		return new TypeSafeMatcher<String>() {
			public void describeTo(Description description) {
				description.appendText("matches ").appendValue(regex);
			}

			@Override
			protected boolean matchesSafely(String item) {
				return (item != null) && item.matches(regex);
			}
		};
	}

	/**
	 * Gets the first found item where there are many.
	 *
	 * @param c
	 * @param matcher
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 * @return an item
	 */
	public static <E> E firstItemOf(Collection<E> c, Matcher<? super E> matcher) {
		for (E item : c) {
			if (matcher.matches(item)) {
				return item;
			}
		}
		return null; // not found
	}

	/**
	 * Gets the zero or one item accepted by the matcher.
	 *
	 * @param c
	 * @param matcher
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 * @return the item or null if not found
	 * @throws AssertionError if multiple items match
	 */
	public static <E> E itemOf(Collection<E> c, Matcher<? super E> matcher) {
		List<E> found = new ArrayList<E>();
		for (E item : c) {
			if (matcher.matches(item)) {
				found.add(item);
			}
		}
		if (found.size() > 1) {
			Description desc = new StringDescription();
			matcher.describeTo(desc);
			throw new AssertionError("Expected at most one item " + desc.toString() + ", got " + found.size() + " items");
		}
		if (found.size() == 1) {
			return found.get(0);
		}
		return null; // not found
	}

	/**
	 * Decorates another Matcher, retaining the behaviour but allowing tests to be
	 * slightly more expressive.
	 * <p>
	 * For example: itemOf(collection, hasName(smelly)) vs. itemOf(collection,
	 * that(hasName(smelly)))
	 */
	public static <T> Matcher<T> that(final Matcher<T> matcher) {
		return new BaseMatcher<T>() {

			@Override
			public boolean matches(Object item) {
				return matcher.matches(item);
			}

			@Override
			public void describeTo(Description description) {
				description.appendText("that ").appendDescriptionOf(matcher);
			}

			@Override
			public void describeMismatch(Object item, Description mismatchDescription) {
				matcher.describeMismatch(item, mismatchDescription);
			}
		};
	}

	public static Matcher<JavaProperty> hasJavaPropertyName(final String name) {
		return new TypeSafeDiagnosingMatcher<JavaProperty>() {
			@Override
			public void describeTo(Description description) {
				description.appendText("has name ").appendValue(name);
			}

			@Override
			protected boolean matchesSafely(JavaProperty item, Description mismatchDescription) {
				if (!name.equals(item.name())) {
					mismatchDescription.appendText("has name ").appendValue(item.name());
					return false;
				}
				return true;
			}
		};
	}

	public static Matcher<JavaProperty> isJavaPropertyField() {
		return new TypeSafeDiagnosingMatcher<JavaProperty>() {
			@Override
			public void describeTo(Description description) {
				description.appendText("is field ");
			}

			@Override
			protected boolean matchesSafely(JavaProperty item, Description mismatchDescription) {
				if (item.isField()) {
					mismatchDescription.appendText("is bean-property");
					return false;
				}
				return true;
			}
		};
	}
}
