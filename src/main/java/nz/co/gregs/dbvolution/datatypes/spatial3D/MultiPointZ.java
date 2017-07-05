/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package nz.co.gregs.dbvolution.datatypes.spatial3D;

import com.vividsolutions.jts.geom.Coordinate;
import com.vividsolutions.jts.geom.CoordinateArrays;
import com.vividsolutions.jts.geom.CoordinateSequence;
import com.vividsolutions.jts.geom.CoordinateSequences;
import com.vividsolutions.jts.geom.Geometry;
import com.vividsolutions.jts.geom.GeometryFactory;
import com.vividsolutions.jts.geom.LineString;
import com.vividsolutions.jts.geom.MultiPoint;
import com.vividsolutions.jts.geom.Point;
import com.vividsolutions.jts.geom.PrecisionModel;
import com.vividsolutions.jts.io.WKTWriter;

/**
 *
 * @author gregorygraham
 */
public class MultiPointZ extends MultiPoint {

	private static final long serialVersionUID = 1L;

	@Deprecated
	@SuppressWarnings("deprecation")
	public MultiPointZ(PointZ[] points, PrecisionModel precisionModel, int SRID) {
		super(points, precisionModel, SRID);
	}

	public MultiPointZ(PointZ[] points, GeometryFactory factory) {
		super(points, factory);
	}

	@Override
	public String toText() {
		WKTWriter writer = new WKTWriter(3);
		return writer.write(this);
	}

	@Override
	public String getGeometryType() {
		return "MultiPointZ";
	}

}
