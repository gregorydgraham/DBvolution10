/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package nz.co.gregs.dbvolution.datatypes.spatial3D;

import com.vividsolutions.jts.geom.Coordinate;
import com.vividsolutions.jts.geom.CoordinateSequence;
import com.vividsolutions.jts.geom.CoordinateSequences;
import com.vividsolutions.jts.geom.GeometryFactory;
import com.vividsolutions.jts.geom.LineString;
import com.vividsolutions.jts.geom.LinearRing;
import com.vividsolutions.jts.geom.MultiPoint;
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

	public PointZ createPointZ(Point point) {
		final Coordinate coord = point.getCoordinate();
		return createPointZ(new Coordinate(coord.x, coord.y, Double.isNaN(coord.z) ? 0.0 : coord.z));
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

	public MultiPointZ createMultiPointZ(CoordinateSequence coordinates) {
		if (coordinates == null) {
			return createMultiPointZ(new PointZ[0]);
		}
		PointZ[] points = new PointZ[coordinates.size()];
		for (int i = 0; i < coordinates.size(); i++) {
			CoordinateSequence ptSeq = getCoordinateSequenceFactory()
					.create(1, coordinates.getDimension());
			CoordinateSequences.copy(coordinates, i, ptSeq, 0, 1);
			if (Double.isNaN(ptSeq.getOrdinate(i, 2))) {
				ptSeq.setOrdinate(i, 2, 0);
			}
			points[i] = createPointZ(ptSeq);
		}

		return createMultiPointZ(points);
	}

	public MultiPointZ createMultiPointZ(Coordinate[] coordinates) {
		return createMultiPointZ(coordinates != null
				? getCoordinateSequenceFactory().create(coordinates)
				: null);
	}

	public MultiPointZ createMultiPointZ(MultiPoint point) {
		return createMultiPointZ(point.getCoordinates());
	}

	public MultiPointZ createMultiPointZ(PointZ[] point) {
		return new MultiPointZ(point, this);
	}

	public MultiPointZ createMultiPointZ(Point[] points) {
		PointZ[] zPoints = new PointZ[points.length];
		for (int i = 0; i < points.length; i++) {
			zPoints[i] = createPointZ(points[i]);
		}
		return createMultiPointZ(zPoints);
	}

}
