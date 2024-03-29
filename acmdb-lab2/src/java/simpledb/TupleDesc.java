package simpledb;

import java.io.Serializable;
import java.util.*;

/**
 * TupleDesc describes the schema of a tuple.
 */
public class TupleDesc implements Serializable {

    /**
     * A help class to facilitate organizing the information of each field
     * */
    public static class TDItem implements Serializable {

        private static final long serialVersionUID = 1L;

        /**
         * The type of the field
         * */
        public final Type fieldType;
        
        /**
         * The name of the field
         * */
        public final String fieldName;

        public TDItem(Type t, String n) {
            this.fieldName = n;
            this.fieldType = t;
        }

        public String toString() {
            return fieldName + "(" + fieldType + ")";
        }
    }

    public List<TDItem> items;

    /**
     * @return
     *        An iterator which iterates over all the field TDItems
     *        that are included in this TupleDesc
     * */
    public Iterator<TDItem> iterator() {
        // some code goes here
        if (items != null) {
            return items.iterator();
        }
        return null;
    }

    private static final long serialVersionUID = 1L;

    /**
     * Create a new TupleDesc with typeAr.length fields with fields of the
     * specified types, with associated named fields.
     * 
     * @param typeAr
     *            array specifying the number of and types of fields in this
     *            TupleDesc. It must contain at least one entry.
     * @param fieldAr
     *            array specifying the names of the fields. Note that names may
     *            be null.
     */
    public TupleDesc(Type[] typeAr, String[] fieldAr) {
        // some code goes here
        int num_items = typeAr.length;
        items = new ArrayList<TDItem>();
        if (fieldAr == null) {
            for (int i = 0; i < num_items; i++) {
                items.add(new TDItem(typeAr[i], null));
            }
        } else {
            for (int i = 0; i < num_items; i++) {
                items.add(new TDItem(typeAr[i], fieldAr[i]));
            }
        }
    }

    /**
     * Constructor. Create a new tuple desc with typeAr.length fields with
     * fields of the specified types, with anonymous (unnamed) fields.
     * 
     * @param typeAr
     *            array specifying the number of and types of fields in this
     *            TupleDesc. It must contain at least one entry.
     */
    public TupleDesc(Type[] typeAr) {
        // some code goes here
        int num_items = typeAr.length;
        items = new ArrayList<TDItem>();

        for (int i = 0; i < num_items; i++) {
            items.add(new TDItem(typeAr[i], null));
        }
    }

    /**
     * @return the number of fields in this TupleDesc
     */
    public int numFields() {
        // some code goes here
        if (items != null) {
            return items.size();
        }
        return 0;
    }

    /**
     * Gets the (possibly null) field name of the ith field of this TupleDesc.
     * 
     * @param i
     *            index of the field name to return. It must be a valid index.
     * @return the name of the ith field
     * @throws NoSuchElementException
     *             if i is not a valid field reference.
     */
    public String getFieldName(int i) throws NoSuchElementException {
        // some code goes here
        if (items != null && items.size() > i) {
            return items.get(i).fieldName;
        }
        throw new NoSuchElementException("Filed Index out of bound");
        //return null;
    }

    /**
     * Gets the type of the ith field of this TupleDesc.
     * 
     * @param i
     *            The index of the field to get the type of. It must be a valid
     *            index.
     * @return the type of the ith field
     * @throws NoSuchElementException
     *             if i is not a valid field reference.
     */
    public Type getFieldType(int i) throws NoSuchElementException {
        // some code goes here
        if (items != null && items.size() > i) {
            return items.get(i).fieldType;
        }
        throw new NoSuchElementException("Filed Index out of bound");
        //return null;
    }

    /**
     * Find the index of the field with a given name.
     * 
     * @param name
     *            name of the field.
     * @return the index of the field that is first to have the given name.
     * @throws NoSuchElementException
     *             if no field with a matching name is found.
     */
    public int fieldNameToIndex(String name) throws NoSuchElementException {
        // some code goes here
        if (items != null) {
            if (name != null) {
                for (int ret = 0; ret < items.size(); ret++) {
                    if (name.equals(items.get(ret).fieldName)) {
                        return ret;
                    }
                }
                throw new NoSuchElementException("No matching filed name");
            } else {
                for (int ret = 0; ret < items.size(); ret++) {
                    if (items.get(ret).fieldName == null) {
                        return ret;
                    }
                }
                throw new NoSuchElementException("No matching filed name");
            }
        }
        throw new NoSuchElementException("No matching filed name");

        //return 0;
    }

    /**
     * @return The size (in bytes) of tuples corresponding to this TupleDesc.
     *         Note that tuples from a given TupleDesc are of a fixed size.
     */
    public int getSize() {
        // some code goes here
        if (items != null) {
            //return items.size();
            //Type stringType = Type.STRING_TYPE;
            int ret = 0;
            for (int i = 0; i < items.size(); i++) {
                ret += items.get(i).fieldType.getLen();
            }
            return ret;
        }

        return 0;
    }

    /**
     * Merge two TupleDescs into one, with td1.numFields + td2.numFields fields,
     * with the first td1.numFields coming from td1 and the remaining from td2.
     * 
     * @param td1
     *            The TupleDesc with the first fields of the new TupleDesc
     * @param td2
     *            The TupleDesc with the last fields of the TupleDesc
     * @return the new TupleDesc
     */
    public static TupleDesc merge(TupleDesc td1, TupleDesc td2) {
        // some code goes here
        //List<TDItem> temp = new ArrayList<TDItem>();
        Type newFieldTypes[];
        String newFiledNames[];
        int num1 = td1.numFields();
        int num2 = td2.numFields();
        newFieldTypes = new Type[num1 + num2];
        newFiledNames = new String[num1 + num2];
        for (int i = 0; i < num1; i++) {
            //temp.add(new TDItem(td1.getFieldType(i), td1.getFieldName(i)));
            newFieldTypes[i] = td1.getFieldType(i);
            newFiledNames[i] = td1.getFieldName(i);

        }
        for (int i = 0; i < num2; i++) {
            //temp.add(new TDItem(td2.getFieldType(i), td2.getFieldName(i)));
            newFieldTypes[i + num1] = td2.getFieldType(i);
            newFiledNames[i + num1] = td2.getFieldName(i);
        }
        return new TupleDesc(newFieldTypes, newFiledNames);
        //return null;
    }

    /**
     * Compares the specified object with this TupleDesc for equality. Two
     * TupleDescs are considered equal if they are the same size and if the n-th
     * type in this TupleDesc is equal to the n-th type in td.
     * 
     * @param o
     *            the Object to be compared for equality with this TupleDesc.
     * @return true if the object is equal to this TupleDesc.
     */
    public boolean equals(Object o) {
        // some code goes here
        if (o instanceof TupleDesc) {
            if (items != null) {
                if (this.getSize() == ((TupleDesc) o).getSize() && items.size() == ((TupleDesc) o).numFields()) {
                    for (int i = 0; i < items.size(); i++) {
                        if (!items.get(i).fieldType.equals(((TupleDesc) o).getFieldType(i))) {
                            return false;
                        }
                    }
                    return true;
                }
                return false;
            } else {
                return (((TupleDesc) o).numFields() == 0);
            }
        }
        return false;
    }

    public int hashCode() {
        // If you want to use TupleDesc as keys for HashMap, implement this so
        // that equal objects have equals hashCode() results
        throw new UnsupportedOperationException("unimplemented");
    }

    /**
     * Returns a String describing this descriptor. It should be of the form
     * "fieldType[0](fieldName[0]), ..., fieldType[M](fieldName[M])", although
     * the exact format does not matter.
     * 
     * @return String describing this descriptor.
     */
    public String toString() {
        // some code goes here
        String ret = "";
        if (items == null) {
            return "Null TupleDesc";
        }
        if (items.size() > 0) {
            ret = String.format(items.get(0).fieldType.toString() + '(' + items.get(0).fieldName + ')');
            for (int i = 1; i < items.size(); i++) {
                ret = ret + String.format(','+ items.get(i).fieldType.toString() + '(' + items.get(i).fieldName + ')');
            }
        }
        return ret;
    }
}
