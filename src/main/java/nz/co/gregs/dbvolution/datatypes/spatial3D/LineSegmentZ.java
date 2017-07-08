/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package nz.co.gregs.dbvolution.datatypes.spatial3D;

import com.vividsolutions.jts.geom.Coordinate;
import com.vividsolutions.jts.geom.CoordinateSequence;
import com.vividsolutions.jts.geom.CoordinateSequences;
import com.vividsolutions.jts.geom.Geometry;
import com.vividsolutions.jts.geom.GeometryFactory;
import com.vividsolutions.jts.geom.LineString;
import com.vividsolutions.jts.geom.PrecisionModel;
import com.vividsolutions.jts.io.WKTWriter;

/**
 *
 * @author gregorygraham
 */
public class LineSegmentZ extends LineString {

	private static final long serialVersionUID = 1L;

	public LineSegmentZ(CoordinateSequence coordinates, GeometryFactory factory) {
		super(coordinates, factory);
	}
	
	public LineSegmentZ(Coordinate... coordinates) {
		super(new GeometryFactory3D().getCoordinateSequenceFactory().create(coordinates), new GeometryFactory3D());
	}

	public LineSegmentZ(double x1, double y1, double z1, double x2, double y2, double z2) {
		super(new GeometryFactory3D().getCoordinateSequenceFactory().create(new Coordinate[]{new Coordinate(x1, y1, z1), new Coordinate(x2, y2, z2)}), new GeometryFactory3D());
	}

	@SuppressWarnings("deprecation")
	@Deprecated
	public LineSegmentZ(Coordinate[] points, PrecisionModel precisionModel, int SRID) {
		super(points, precisionModel, SRID);
	}

	@Override
	public String toText() {
		WKTWriter writer = new WKTWriter(3);
		return writer.write(this);
	}

	@Override
	public String getGeometryType() {
		return "LineSegmentZ";
	}

	@Override
	public Geometry reverse() {
		CoordinateSequence seq = (CoordinateSequence) points.clone();
		CoordinateSequences.reverse(seq);
		LineStringZ revLine = new GeometryFactory3D().createLineStringZ(seq);
		return revLine;
	}

	public Coordinate getCoordinate(int i) {
		return getCoordinateN(i);
	}
}
