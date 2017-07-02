package knapp.spec;

/**
 * Created by michael on 7/1/17.
 */
public class CSVColumn {
    private String name;
    private String type;
    private boolean keepRange;
    private boolean bloomFilter;
    private int sort;
    private int number;

    public static CSVColumn fromLine(String line,int number) {
        String[] parts = line.split(",");
        CSVColumn csvColumn = new CSVColumn();
        csvColumn.number = number;
        csvColumn.name = parts[0].trim();
        csvColumn.type = parts[1].trim();
        csvColumn.keepRange = Boolean.parseBoolean(parts[2]);
        csvColumn.bloomFilter = Boolean.parseBoolean(parts[3]);
        csvColumn.sort = Integer.parseInt(parts[4]);
        return csvColumn;
    }

    public CSVColumn() {

    }

    public int getNumber() {
        return number;
    }

    public void setNumber(int number) {
        this.number = number;
    }

    public int getSort() {
        return sort;
    }

    public void setSort(int sort) {
        this.sort = sort;
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
