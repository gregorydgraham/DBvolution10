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
import java.util.logging.Level;
import java.util.logging.Logger;
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
	protected int debugMode = 0; // 1: textual indication of change, 2: difference of factors
	private boolean compared = false;

	/* create a runable demo. */
//	public static void main(String[] args) {
//		// Create a compare object specifying the 2 images for comparison.
//		ImageCompare ic = new ImageCompare("c:\\test1.jpg", "c:\\test2.jpg");
//		// Set the comparison parameters. 
//		//   (num vertical regions, num horizontal regions, sensitivity, stabilizer)
//		ic.setParameters(8, 6, 5, 10);
//		// Display some indication of the differences in the image.
//		ic.setDebugMode(2);
//		// Compare.
//		ic.compare();
//		// Display if these images are considered a match according to our parameters.
//		System.out.println("Match: " + ic.match());
//		// If its not a match then write a file to show changed regions.
//		if (!ic.match()) {
//			saveJPG(ic.getChangeIndicator(), "c:\\changes.jpg");
//		}
//	}
	// constructor 1. use filenames
	public ImageCompare(String file1, String file2) throws IOException {
		this(loadJPG(file1), loadJPG(file2));
	}

	// constructor 1b. use files
	public ImageCompare(File file1, File file2) throws IOException {
		this(loadJPG(file1.getAbsolutePath()), loadJPG(file2.getAbsolutePath()));
	}

	// constructor 2. use awt images.
	public ImageCompare(Image img1, Image img2) {
		this(imageToBufferedImage(img1), imageToBufferedImage(img2));
	}

	// constructor 3. use buffered images. all roads lead to the same place. this place.
	public ImageCompare(BufferedImage img1, BufferedImage img2) {
		this.original1 = img1;
		this.original2 = img2;
		autoSetParameters();
	}

	// like this to perhaps be upgraded to something more heuristic in the future.
	protected synchronized final void autoSetParameters() {
		compared=false;
		imageDiff=null;
		comparex = 10;
		comparey = 10;
		factorA = 10;
		factorD = 10;
	}

	// set the parameters for use during change detection.
	public synchronized void setParameters(int x, int y, int factorA, int factorD) {
		compared=false;
		imageDiff=null;
		this.comparex = x;
		this.comparey = y;
		this.factorA = factorA;
		this.factorD = factorD;
	}

	// want to see some stuff in the console as the comparison is happening?
	public void setDebugMode(int m) {
		compared=false;
		imageDiff=null;
		this.debugMode = m;
	}

	// compare the two images in this object.
	public void compare() {
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

	// return the image that indicates the regions where changes where detected.
	public BufferedImage getChangeIndicator() {
		return imageDiff;
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
	public boolean match() {
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

	// write a buffered image to a jpeg file.
	protected static void saveJPG(Image img, String filename) {
		BufferedImage bi = imageToBufferedImage(img);
		try {
			FileOutputStream out = new FileOutputStream(filename);
			ImageIO.write(bi, "jpg", out);
		} catch (java.io.FileNotFoundException io) {
			System.out.println("File Not Found");
		} catch (IOException ex) {
			Logger.getLogger(ImageCompare.class.getName()).log(Level.SEVERE, null, ex);
		}
	}

	// read a jpeg file into a buffered image
	protected static Image loadJPG(String filename) throws IOException {
		FileInputStream in = null;
		try {
			in = new FileInputStream(filename);
		} catch (java.io.FileNotFoundException io) {
			System.out.println("File Not Found");
		}
		BufferedImage bi = ImageIO.read(new File(filename));
		return bi;
	}

}
