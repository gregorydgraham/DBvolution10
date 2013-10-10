package nz.co.gregs.dbvolution;

public interface DBTypeAdaptor {
	public Object toObjectValue(Object dbvValue);

	public Object toDBvValue(Object objectValue);
}
