/**
 * Originally written by Pat Cullen
 * http://mindmeat.blogspot.com/2008/07/java-image-comparison.html
 *
 * Updated to use ImageIO by Gregory Graham
 */
package nz.co.gregs.dbvolution.utility;

import javax.swing.*;
import java.io.*;
import java.awt.*;
import java.awt.image.*;
import javax.imageio.ImageIO;

public class ImageCompare {

	protected final BufferedImage original1;
	protected final BufferedImage original2;
	protected BufferedImage imageDiff = null;
	protected int comparex = 0;
	protected int comparey = 0;
	protected int factorA = 0;
	protected int factorD = 10;
	protected boolean match = false;
	protected int debugMode = 0; 
	private boolean compared = false;

	public ImageCompare(File file1, File file2) throws IOException {
		this(loadJPG(file1.getAbsolutePath()), loadJPG(file2.getAbsolutePath()));
	}

	public ImageCompare(Image img1, Image img2) {
		this(imageToBufferedImage(img1), imageToBufferedImage(img2));
	}

	protected ImageCompare(BufferedImage img1, BufferedImage img2) {
		this.original1 = img1;
		this.original2 = img2;
		autoSetParameters();
	}

	protected synchronized final void autoSetParameters() {
		compared = false;
		imageDiff = null;
		comparex = 10;
		comparey = 10;
		factorA = 10;
		factorD = 10;
	}

	public synchronized void setParameters(int x, int y, int factorA, int factorD) {
		compared = false;
		imageDiff = null;
		this.comparex = x;
		this.comparey = y;
		this.factorA = factorA;
		this.factorD = factorD;
	}

	// compare the two images in this object.
	public synchronized void compare() {
		// setup change display image
		imageDiff = imageToBufferedImage(original2);
		Graphics2D gc = imageDiff.createGraphics();
		gc.setColor(Color.RED);
		// convert to gray images.
		BufferedImage img1 = imageToBufferedImage(GrayFilter.createDisabledImage(original1));
		BufferedImage img2 = imageToBufferedImage(GrayFilter.createDisabledImage(original2));
		// how big are each section
		int blocksx = img1.getWidth() / comparex;
		int blocksy = img1.getHeight() / comparey;
		// set to a match by default, if a change is found then flag non-match
		this.match = true;
		// loop through whole image and compare individual blocks of images
		for (int y = 0; y < comparey; y++) {
			if (debugMode > 0) {
				System.out.print("|");
			}
			for (int x = 0; x < comparex; x++) {
				int b1 = getAverageBrightness(img1.getSubimage(x * blocksx, y * blocksy, blocksx - 1, blocksy - 1));
				int b2 = getAverageBrightness(img2.getSubimage(x * blocksx, y * blocksy, blocksx - 1, blocksy - 1));
				int diff = Math.abs(b1 - b2);
				if (diff > factorA) { // the difference in a certain region has passed the threshold value of factorA
					// draw an indicator on the change image to show where change was detected.
					gc.drawRect(x * blocksx, y * blocksy, blocksx - 1, blocksy - 1);
					this.match = false;
				}
				if (debugMode == 1) {
					System.out.print((diff > factorA ? "X" : " "));
				}
				if (debugMode == 2) {
					System.out.print(diff + (x < comparex - 1 ? "," : ""));
				}
			}
			if (debugMode > 0) {
				System.out.println("|");
			}
		}
		compared = true;
	}

	// returns a value specifying some kind of average brightness in the image.
	protected int getAverageBrightness(BufferedImage img) {
		Raster r = img.getData();
		int total = 0;
		for (int y = 0; y < r.getHeight(); y++) {
			for (int x = 0; x < r.getWidth(); x++) {
				total += r.getSample(r.getMinX() + x, r.getMinY() + y, 0);
			}
		}
		return total / ((r.getWidth() / factorD) * (r.getHeight() / factorD));
	}

	// returns true if image pair is considered a match
	public synchronized boolean match() {
		if (!compared) {
			compare();
		}
		return match;
	}

	// buffered images are just better.
	protected static BufferedImage imageToBufferedImage(Image img) {
		BufferedImage bi = new BufferedImage(img.getWidth(null), img.getHeight(null), BufferedImage.TYPE_INT_RGB);
		Graphics2D g2 = bi.createGraphics();
		g2.drawImage(img, null, null);
		return bi;
	}

	// read a jpeg file into a buffered image
	protected static Image loadJPG(String filename) throws IOException {
		BufferedImage bi = ImageIO.read(new File(filename));
		return bi;
	}

}
