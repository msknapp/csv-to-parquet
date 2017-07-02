package knapp.domain;

/**
 * Created by michael on 7/1/17.
 */
public class CSVColumn {
    private String name;
    private String type;
    private boolean keepRange;
    private boolean bloomFilter;
    private int columnNumber;
    private int sortNumber;
    private SortOrder sortOrder;

    public static CSVColumn fromLine(String line, int number) {
        String[] parts = line.split(",");
        CSVColumn csvColumn = new CSVColumn();
        csvColumn.columnNumber = number;
        csvColumn.name = parts[0].trim();
        csvColumn.type = parts[1].trim();
        csvColumn.keepRange = Boolean.parseBoolean(parts[2]);
        csvColumn.bloomFilter = Boolean.parseBoolean(parts[3]);
        csvColumn.sortNumber = Integer.parseInt(parts[4]);
        csvColumn.sortOrder = SortOrder.parse(parts[5]);
        return csvColumn;
    }

    public enum SortOrder {
        ASCENDING,
        DESCENDING,
        UNSORTED;

        public static SortOrder parse(String s) {
            if (s.trim().toLowerCase().startsWith("des")) {
                return DESCENDING;
            } else if (s.trim().toLowerCase().startsWith("asc")) {
                return ASCENDING;
            } else {
                return UNSORTED;
            }
        }
    }

    public CSVColumn() {

    }

    public int getColumnNumber() {
        return columnNumber;
    }

    public void setColumnNumber(int columnNumber) {
        this.columnNumber = columnNumber;
    }

    public int getSortNumber() {
        return sortNumber;
    }

    public void setSortNumber(int sortNumber) {
        this.sortNumber = sortNumber;
    }

    public SortOrder getSortOrder() {
        return sortOrder;
    }

    public void setSortOrder(SortOrder sortOrder) {
        this.sortOrder = sortOrder;
    }

    public int getNumber() {
        return columnNumber;
    }

    public void setNumber(int number) {
        this.columnNumber = number;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getType() {
        return type;
    }

    public void setType(String type) {
        this.type = type;
    }

    public boolean isKeepRange() {
        return keepRange;
    }

    public void setKeepRange(boolean keepRange) {
        this.keepRange = keepRange;
    }

    public boolean isBloomFilter() {
        return bloomFilter;
    }

    public void setBloomFilter(boolean bloomFilter) {
        this.bloomFilter = bloomFilter;
    }
}
