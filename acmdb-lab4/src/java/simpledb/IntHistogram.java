package simpledb;

/** A class to represent a fixed-width histogram over a single integer-based field.
 */
public class IntHistogram {

    /**
     * Create a new IntHistogram.
     * 
     * This IntHistogram should maintain a histogram of integer values that it receives.
     * It should split the histogram into "buckets" buckets.
     * 
     * The values that are being histogrammed will be provided one-at-a-time through the "addValue()" function.
     * 
     * Your implementation should use space and have execution time that are both
     * constant with respect to the number of values being histogrammed.  For example, you shouldn't 
     * simply store every value that you see in a sorted list.
     * 
     * @param buckets The number of buckets to split the input value into.
     * @param min The minimum integer value that will ever be passed to this class for histogramming
     * @param max The maximum integer value that will ever be passed to this class for histogramming
     */
    int[] bucket;
    int min, max;
    int len;
    int w;
    public IntHistogram(int buckets, int min, int max) {
    	// some code goes here
        len = buckets;
        bucket = new int[buckets + 1];
        for (int i = 0; i <= buckets; i++) {
            bucket[i] = 0;
        }
        this.min = min;
        this.max = max + 1;
        if (this.max - this.min < len) {
            w = 1;
        } else {
            w = (this.max - this.min) / len;
        }
    }


    private int lowbit(int x) {
        return x & (-x);
    }
    private int getSum(int x) {

        int ret = 0;
        if (x < 0) {
            return 0;
        }
        if (x > len) {
            x = len;
        }
        for (int i = x; i > 0; i -= lowbit(i)) {
            ret += bucket[i];
        }
        return ret;
    }
    /**
     * Add a value to the set of values that you are keeping a histogram of.
     * @param v Value to add to the histogram
     */
    public void addValue(int v) {
    	// some code goes here

        int pos = (v - min) / w;

        for (int i = pos + 1; i <= len; i += lowbit(i)) {
            bucket[i] += 1;
        }
    }

    /**
     * Estimate the selectivity of a particular predicate and operand on this table.
     * 
     * For example, if "op" is "GREATER_THAN" and "v" is 5, 
     * return your estimate of the fraction of elements that are greater than 5.
     * 
     * @param op Operator
     * @param v Value
     * @return Predicted selectivity of this particular operator and value
     */
    public double estimateSelectivity(Predicate.Op op, int v) {

    	// some code goes here
       // System.out.println("v="+v);
        int pos =(int)((v - min) / w);

        if (v < min) {
            pos = -1;
        }
        if (v > max - 1) {
            pos = len;
        }

        double l = min + w * pos, r = min + w * pos + w - 1;
        int total = getSum(len);
        int numv = getSum(pos + 1) - getSum(pos);
        double f = -0.1;
        //System.out.println("ss + " + min + " " + max + " " + len + " " + v + " " + pos + " " + l + " " + r + " " + numv + " " + total + " w= " + w);
        //System.out.println("l = " + l + " r=" + r);
        if (op.equals(Predicate.Op.GREATER_THAN)) {
            f = ((r * 1.0 - v) / w * numv + total - getSum(pos + 1)) / total;
            //System.out.println(f);
           // System.out.println("aa  " + r + " " + v + " " + f +" "+ + total);
        }
        if (op.equals(Predicate.Op.EQUALS)) {
            f = (1.0 / w * numv) / total;
        }
        if (op.equals(Predicate.Op.GREATER_THAN_OR_EQ)) {
            f = ((r * 1.0 - v + 1) / w * numv + total - getSum(pos + 1)) / total;
        }
        if (op.equals(Predicate.Op.LESS_THAN)) {
            f = ((v * 1.0 - l) / w * numv + getSum(pos)) / total;
        }
        if (op.equals(Predicate.Op.LESS_THAN_OR_EQ)) {
            f = ((v * 1.0 - l + 1) / w * numv + getSum(pos)) / total;
        }
        if (op.equals(Predicate.Op.NOT_EQUALS)) {
            f = (total - (1.0 / w) * numv) / total;
        }
        return f;
    }
    
    /**
     * @return
     *     the average selectivity of this histogram.
     *     
     *     This is not an indispensable method to implement the basic
     *     join optimization. It may be needed if you want to
     *     implement a more efficient optimization
     * */
    public double avgSelectivity()
    {
        // some code goes here
        return 1.0;
    }
    
    /**
     * @return A string describing this histogram, for debugging purposes
     */
    public String toString() {
        // some code goes here
        return String.format("Histogram(range(%d,%d), total(%d))", min, max, getSum(len));
    }
}
