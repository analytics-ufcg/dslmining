
package api.measures;

import org.apache.mahout.math.Vector;

import java.util.Iterator;

public class PearsonCorrelationSimilarity extends org.apache.mahout.math.hadoop.similarity.cooccurrence.measures.CosineSimilarity {

  @Override
  public Vector normalize(Vector vector) {
    if (vector.getNumNondefaultElements() == 0) {
      return vector;
    }
    // center non-zero elements
    double average = vector.norm(1) / vector.getNumNondefaultElements();
    Iterator<Vector.Element> nonZeroElements = vector.nonZeroes().iterator();
    while (nonZeroElements.hasNext()) {
      Vector.Element nonZeroElement = nonZeroElements.next();
      vector.setQuick(nonZeroElement.index(), nonZeroElement.get() - average);
    }
    return super.normalize(vector);
  }
}
