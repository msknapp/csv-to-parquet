package knapp.read;

import knapp.domain.CSVColumn;
import org.apache.parquet.io.api.Binary;
import org.apache.parquet.io.api.PrimitiveConverter;

import java.io.UnsupportedEncodingException;

/**
 * Created by michael on 7/2/17.
 */
class CSVColumnConverter extends PrimitiveConverter {

    private CSVGroupConverter CSVGroupConverter;
    private CSVColumn csvColumn;

    public CSVColumnConverter(CSVGroupConverter CSVGroupConverter, CSVColumn csvColumn) {
        this.CSVGroupConverter = CSVGroupConverter;
        this.csvColumn = csvColumn;
    }

    @Override
    public boolean isPrimitive() {
        return true;
    }

    public void addBinary(Binary value) {
        try {
            CSVGroupConverter.getCurrentValue()[csvColumn.getNumber()] = new String(value.getBytes(), "UTF-8");
        } catch (UnsupportedEncodingException e) {
            e.printStackTrace();
        }
    }

    /**
     * @param value value to set
     */
    public void addBoolean(boolean value) {
        CSVGroupConverter.getCurrentValue()[csvColumn.getNumber()] = String.valueOf(value);
    }

    /**
     * @param value value to set
     */
    public void addDouble(double value) {
        CSVGroupConverter.getCurrentValue()[csvColumn.getNumber()] = String.valueOf(value);
    }

    /**
     * @param value value to set
     */
    public void addFloat(float value) {
        CSVGroupConverter.getCurrentValue()[csvColumn.getNumber()] = String.valueOf(value);
    }

    /**
     * @param value value to set
     */
    public void addInt(int value) {
        CSVGroupConverter.getCurrentValue()[csvColumn.getNumber()] = String.valueOf(value);
    }

    public void addLong(long value) {
        CSVGroupConverter.getCurrentValue()[csvColumn.getNumber()] = String.valueOf(value);
    }
}
