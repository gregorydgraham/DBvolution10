/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package nz.co.gregs.dbvolution.datatypes.spatial3D;

import com.vividsolutions.jts.geom.Coordinate;
import com.vividsolutions.jts.geom.CoordinateSequence;
import com.vividsolutions.jts.geom.GeometryFactory;
import com.vividsolutions.jts.geom.LineString;
import com.vividsolutions.jts.geom.LinearRing;
import com.vividsolutions.jts.geom.Point;
import com.vividsolutions.jts.geom.Polygon;

/**
 *
 * @author gregorygraham
 */
public class GeometryFactory3D extends GeometryFactory {

	private static final long serialVersionUID = 1L;

	public PointZ createPointZ(CoordinateSequence coordinates) {
		return new PointZ(coordinates, this); //To change body of generated methods, choose Tools | Templates.
	}

	public PointZ createPointZ(Coordinate coordinate) {
		return new PointZ(coordinate != null ? getCoordinateSequenceFactory().create(new Coordinate[]{coordinate}) : null, this);
	}

	public LineStringZ createLineStringZ(LineString line) {
		return createLineStringZ(line.getCoordinateSequence());
	}

	public LineStringZ createLineStringZ(CoordinateSequence coordinates) {
		return new LineStringZ(coordinates, this);
	}

	public LineStringZ createLineStringZ(Coordinate[] coordinates) {
		return createLineStringZ(coordinates != null ? getCoordinateSequenceFactory().create(coordinates) : null);
	}

	public LinearRing createLinearRingZ(CoordinateSequence coordinates) {
		return new LinearRingZ(coordinates, this);
	}

	public LinearRingZ createLinearRingZ(Coordinate[] coordinates) {
		return new LinearRingZ(coordinates);
	}

	public PolygonZ createPolygonZ(Coordinate[] coords) {
		return createPolygonZ(createLinearRing(coords));
	}

	public PolygonZ createPolygonZ(LinearRing shell) {
		return createPolygonZ(shell, null);
	}

	public PolygonZ createPolygonZ(CoordinateSequence coordinates) {
		return createPolygonZ(createLinearRing(coordinates));
	}

	public PolygonZ createPolygonZ(LinearRing shell, LinearRing[] holes) {
		return new PolygonZ(shell, holes, this);
	}

	public PolygonZ createPolygonZ(Polygon poly) {
		LinearRingZ shell = this.createLinearRingZ(poly.getExteriorRing().getCoordinates());
		LinearRingZ[] holes = null;
		final int numInteriorRing = poly.getNumInteriorRing();
		if (numInteriorRing > 0) {
			holes = new LinearRingZ[numInteriorRing];
			for (int i = 0; i < numInteriorRing; i++) {
				LineStringZ interiorRingN = this.createLineStringZ(poly.getInteriorRingN(i));
				holes[i] = this.createLinearRingZ(interiorRingN.getCoordinates());
			}
		}
		return createPolygonZ(shell, holes);
	}

}
