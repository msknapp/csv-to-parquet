package knapp.data;

/**
 * Created by michael on 7/1/17.
 */
public class Row implements Comparable<Row> {

    private int[] sortColumns;
    private String[] cells;

    public Row(int ... sortColumns) {
        this.sortColumns = sortColumns;
    }

    public String[] getCells() {
        return cells;
    }

    public void setCells(String[] cells) {
        this.cells = cells;
    }

    @Override
    public int compareTo(Row o) {
        if (sortColumns.length < 1) {
            return 0;
        }
        for (int i = 0;i<sortColumns.length;i++) {
            int sortColNumber = sortColumns[i];
            String myVal = cells[sortColNumber];
            String theirVal = o.cells[sortColNumber];
            if (myVal == null && theirVal == null){
                continue;
            }
            if (myVal == null) {
                return -1;
            }
            if (theirVal == null) {
                return 1;
            }
            if (myVal.matches("[\\d\\.]+") && theirVal.matches("[\\d\\.]+")) {
                // numeric comparison
                double d1 = Double.parseDouble(myVal);
                double d2 = Double.parseDouble(theirVal);
                if (Math.abs(d2-d1) < 1e-5) {
                    continue;
                } else if (d1 > d2) {
                    return 1;
                } else {
                    return -1;
                }
            }
            if (myVal.equals(theirVal)) {
                continue;
            } else {
                return myVal.compareTo(theirVal);
            }
        }
        return 0;
    }
}
