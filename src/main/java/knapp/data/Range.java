package knapp.data;

/**
 * Created by michael on 7/1/17.
 */
public class Range {
    final String min;
    final String max;

    public Range(String min, String max) {
        this.min = min;
        this.max = max;
    }

    public Range(int min, int max) {
        this.min = String.valueOf(min);
        this.max = String.valueOf(max);
    }

    public Range(double min, double max) {
        this.min = String.valueOf(min);
        this.max = String.valueOf(max);
    }

    public Range(boolean min, boolean max) {
        this.min = String.valueOf(min);
        this.max = String.valueOf(max);
    }

    public Range(long min, long max) {
        this.min = String.valueOf(min);
        this.max = String.valueOf(max);
    }

    public String getMin() {
        return min;
    }

    public String getMax() {
        return max;
    }
}
