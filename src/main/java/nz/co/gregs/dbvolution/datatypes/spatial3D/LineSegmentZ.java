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

	public Geometry intersection(LineSegmentZ other) {
		if (this.isEmpty() || other.isEmpty()) {
			return new LineSegmentZ();
		}
		final Coordinate p0 = this.getCoordinate(0);
		final Coordinate p1 = this.getCoordinate(1);
		double p0x = p0.getOrdinate(0);
		double p0y = p0.getOrdinate(1);
		double p0z = p0.getOrdinate(2);
		double p1x = p1.getOrdinate(0);
		double p1y = p1.getOrdinate(1);
		double p1z = p1.getOrdinate(2);

		final Coordinate p2 = other.getCoordinate(0);
		final Coordinate p3 = other.getCoordinate(1);
		double p2x = p2.getOrdinate(0);
		double p2y = p2.getOrdinate(1);
		double p2z = p2.getOrdinate(2);
		double p3x = p3.getOrdinate(0);
		double p3y = p3.getOrdinate(1);
		double p3z = p3.getOrdinate(2);

		double s1_x, s1_y, s2_x, s2_y;
		double i_x, i_y;
		s1_x = p1x - p0x;
		s1_y = p1y - p0y;
		s2_x = p3x - p2x;
		s2_y = p3y - p2y;

		double s, t;

		s = (-s1_y * (p0x - p2x) + s1_x * (p0y - p2y)) / (-s2_x * s1_y + s1_x * s2_y);
		t = (s2_x * (p0y - p2y) - s2_y * (p0x - p2x)) / (-s2_x * s1_y + s1_x * s2_y);

		if (s >= 0 && s <= 1 && t >= 0 && t <= 1) {
			double s1_z = p1z - p0z;
			double s2_z = p3z - p2z;
			double t_z = p0z + (t * s1_z);
			double s_z = p2z + (s * s2_z);
			if (t_z == s_z) {
				// t and s create the same z so there is an inersection\n"
				// Collision detected\n"
				i_x = p0x + (t * s1_x);
				i_y = p0y + (t * s1_y);
				return new PointZ(
						new GeometryFactory3D()
						.getCoordinateSequenceFactory()
						.create(new Coordinate[]{new Coordinate(i_x, i_y, t_z)}), 
						new GeometryFactory3D()
				);
			} else {
				return new LineSegmentZ();
			}
		} else {
			// No collision\n"
			return new LineSegmentZ();
		}
	}

}
