package uga.cs4370.mydbimpl;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import uga.cs4370.mydb.Cell;
import uga.cs4370.mydb.Predicate;
import uga.cs4370.mydb.RA;
import uga.cs4370.mydb.Relation;
import uga.cs4370.mydb.RelationBuilder;
import uga.cs4370.mydb.Type;

/**
 * Implementation of the Relation Algebra operators class.
 */
public class RAimpl implements RA {

    /**
     * @inheritDoc
     */
    public Relation select(Relation rel, Predicate p) {
        // Create a new relation similar to the way it was given
        Relation newRel = new RelationBuilder()
                .attributeNames(rel.getAttrs())
                .attributeTypes(rel.getTypes())
                .build();

        // Iterate through each row of the given relation
        for (int i = 0; i < rel.getSize(); i++) {
            // Retrieve the row at current index
            List<Cell> row = rel.getRow(i);

            // Check if the row satisfies the predicate
            if (p.check(row)) {
                // Insert the row into the new relation
                newRel.insert(row);
            }
        }

        // Return the new relation containing the selected rows
        return newRel;
    }

    // union
    /**
     * {@inheritDoc}
     */
    public Relation union(Relation rel1, Relation rel2) {
        List<List<Cell>> combinedRows = new ArrayList<>(rel1.getSize() + rel2.getSize());
        combinedRows.addAll(getAllRows(rel1));
        combinedRows.addAll(getAllRows(rel2));

        List<Type> types = rel1.getTypes();
        List<String> attributes = rel1.getAttrs();

        RelationBuilder builder = new RelationBuilder();
        Relation result = builder.attributeNames(attributes)
                .attributeTypes(types)
                .build();

        for (List<Cell> row : combinedRows) {
            result.insert(row);
        }

        return result; 
    } // union

    // diff
    /**
     * {@inheritDoc}
     */
    public Relation diff(Relation rel1, Relation rel2) {
        List<List<Cell>> diffRows = new ArrayList<>(rel1.getSize());
        for (List<Cell> row : getAllRows(rel1)) {
            if (!containsRow(rel2, row)) {
                diffRows.add(new ArrayList<>(row));
            }
        }

        List<Type> types = rel1.getTypes();
        List<String> attributes = rel1.getAttrs();

        RelationBuilder builder = new RelationBuilder();
        Relation result = builder.attributeNames(attributes)
                .attributeTypes(types)
                .build();

        for (List<Cell> row : diffRows) {
            result.insert(row);
        }

        return result;
    } // diff

    /**
     * @inheritDoc
     */
    public Relation rename(Relation rel, List<String> origAttr, List<String> renamedAttr) {

        // creates new empty relation to store results
        Relation newRel = new RelationBuilder().build();

        // [CONDITION 1] checks if argument counts match, if not then throw
        // IllegalArgumentException
        if (origAttr.size() != renamedAttr.size()) {
            throw new IllegalArgumentException("Argument counts do not match.\n");
        }

        // [CONDITION 2] checks if each attribute in origAttr is present in rel, if not
        // then throw IllegalArgumentException

        List<String> relAttrs = rel.getAttrs();
        List<Type> relTypes = rel.getTypes();
        int matchingAttrsCount = 0;

        // counts how many attributes from origAttr match with rel
        for (int i = 0; i < relAttrs.size(); i++) {
            for (int k = 0; k < origAttr.size(); k++) {
                if (relAttrs.get(i).compareTo(origAttr.get(k)) == 0) {

                    matchingAttrsCount++;
                }
            }
        }

        if (matchingAttrsCount != relAttrs.size()) {
            throw new IllegalArgumentException("Not all attributes in origAttr are present in rel.\n");

        } else {
            // [PASSED ALL CONDITIONS] if relation passes both exceptions above, then method
            // will rename the attributes
            // Get the original attributes and types

            List<String> originalAttributes = rel.getAttrs();
            List<Type> originalTypes = rel.getTypes();

            // Create a new RelationBuilder instance
            RelationBuilder builder = new RelationBuilder();

            // Initialize lists to store renamed attributes and types
            List<String> newAttrNames = new ArrayList<>();
            List<Type> newAttrTypes = new ArrayList<>();

            // Iterate over original attributes
            for (int i = 0; i < originalAttributes.size(); i++) {
                String originalAttr = originalAttributes.get(i);
                Type originalType = originalTypes.get(i);

                // If the attribute needs to be renamed
                if (origAttr.contains(originalAttr)) {
                    int index = origAttr.indexOf(originalAttr);
                    // Add the renamed attribute and its type to the lists
                    newAttrNames.add(renamedAttr.get(index));
                    newAttrTypes.add(originalType);
                } else {
                    // Otherwise, keep the original attribute and its type
                    newAttrNames.add(originalAttr);
                    newAttrTypes.add(originalType);
                }
            }

            // Set the new attribute names and types in the builder
            builder.attributeNames(newAttrNames).attributeTypes(newAttrTypes);

            // Build the new relation
            newRel = builder.build();

            // Print the new resulting relation
            //newRel.print();

            return newRel;
        }
    }

    /**
     * @inheritDoc
     */
    public Relation join(Relation rel1, Relation rel2) {
        List<String> commonAttributes = new ArrayList<>();
        List<String> allAttributes = new ArrayList<>();
        for (String attribute : rel1.getAttrs()) {
            if (rel2.hasAttr(attribute)) {
                commonAttributes.add(attribute);
            }
            allAttributes.add(attribute);
        }
        for (String attribute : rel2.getAttrs()) {
            if (!allAttributes.contains(attribute)) {
                allAttributes.add(attribute);
            }
        }

        List<Type> allTypes = new ArrayList<>();
        for (String attribute : allAttributes) {
            if (rel1.hasAttr(attribute)) {
                allTypes.add(rel1.getTypes().get(rel1.getAttrIndex(attribute)));
            } else {
                allTypes.add(rel2.getTypes().get(rel2.getAttrIndex(attribute)));
            }
        }

        Relation ret = new RelationBuilder()
                .attributeNames(allAttributes)
                .attributeTypes(allTypes)
                .build();

        int size1 = rel1.getSize();
        int size2 = rel2.getSize();
        for (int i = 0; i < size1; i++) {
            for (int j = 0; j < size2; j++) {
                boolean matches = true;
                for (String attribute : commonAttributes) {
                    Cell cell1 = rel1.getRow(i).get(rel1.getAttrIndex(attribute));
                    Cell cell2 = rel2.getRow(j).get(rel2.getAttrIndex(attribute));
                    if (!cell1.equals(cell2)) {
                        matches = false;
                        break;
                    }
                }

                if (matches) {
                    List<Cell> newRow = new ArrayList<>();
                    for (String attribute : allAttributes) {
                        if (rel1.hasAttr(attribute)) {
                            newRow.add(rel1.getRow(i).get(rel1.getAttrIndex(attribute)));
                        } else if (rel2.hasAttr(attribute)) {
                            newRow.add(rel2.getRow(j).get(rel2.getAttrIndex(attribute)));
                        }
                    }
                    ret.insert(newRow);
                }
            }
        }
        return ret;
    }

    /**
     * @inheritDoc
     */
    public Relation join(Relation rel1, Relation rel2, Predicate predicate) {
        List<String> allAttributes = new ArrayList<>();
        for (String attribute : rel1.getAttrs()) {
            if (!allAttributes.contains(attribute)) {
                allAttributes.add(attribute);
            }
        }
        for (String attribute : rel2.getAttrs()) {
            if (!allAttributes.contains(attribute)) {
                allAttributes.add(attribute);
            }
        }
        List<Type> allTypes = new ArrayList<>();
        for (String attribute : allAttributes) {
            if (rel1.hasAttr(attribute)) {
                allTypes.add(rel1.getTypes().get(rel1.getAttrIndex(attribute)));
            } else {
                allTypes.add(rel2.getTypes().get(rel2.getAttrIndex(attribute)));
            }
        }

        Relation ret = new RelationBuilder()
                .attributeNames(allAttributes)
                .attributeTypes(allTypes)
                .build();

        int size1 = rel1.getSize();
        int size2 = rel2.getSize();
        for (int i = 0; i < size1; i++) {
            for (int j = 0; j < size2; j++) {
                List<Cell> combined = new ArrayList<>();
                for (String attribute : allAttributes) {
                    if (rel1.hasAttr(attribute)) {
                        combined.add(rel1.getRow(i).get(rel1.getAttrIndex(attribute)));
                    } else if (rel2.hasAttr(attribute)) {
                        combined.add(rel2.getRow(j).get(rel2.getAttrIndex(attribute)));
                    }
                }

                if (predicate.check(combined)) {
                    ret.insert(combined);
                }
            }
        }
        return ret;
    }

    /**
     * Helper method to get all rows from a relation.
     * 
     * @param relation The input relation that the rows are selected from.
     * @return rows
     */
    private List<List<Cell>> getAllRows(Relation relation) {
        List<List<Cell>> rows = new ArrayList<>();
        for (int i = 0; i < relation.getSize(); i++) {
            rows.add(relation.getRow(i));
        }
        return rows;
    } // getAllRows

    /**
     * Helper method to check if a relation contains a specific row.
     * 
     * @param relation The input relation to check.
     * @param row      The given row to check against the relation.
     * @return true or false depending on if the relation contains the row.
     */
    private boolean containsRow(Relation relation, List<Cell> row) {
        for (int i = 0; i < relation.getSize(); i++) {
            if (relation.getRow(i).equals(row)) {
                return true;
            }
        }
        return false;
    } // containsRow

    /**
     * @inheritDoc
     */
    public Relation cartesianProduct(Relation rel1, Relation rel2) {
        List<List<Cell>> resultRows = new ArrayList<>();

        // Check if rel1 and rel2 have common attributes
        if (haveCommonAttributes(rel1, rel2)) {
            throw new IllegalArgumentException("Relations have common attributes.");
        }

        List<String> newAttributes = new ArrayList<>(rel1.getAttrs());
        newAttributes.addAll(rel2.getAttrs());

        // Expand the schema size to the product of rel1 attributes times rel2
        // attributes
        int newSchemaSize = newAttributes.size();

        List<Type> newTypes = new ArrayList<>(rel1.getTypes());
        newTypes.addAll(rel2.getTypes());

        // Create a new relation using RelationBuilder
        RelationBuilder builder = new RelationBuilder()
                .attributeNames(newAttributes)
                .attributeTypes(newTypes);

        Relation newRelation = builder.build();

        for (int i = 0; i < rel1.getSize(); i++) {
            List<Cell> row1 = rel1.getRow(i);

            for (int j = 0; j < rel2.getSize(); j++) {
                List<Cell> row2 = rel2.getRow(j);

                List<Cell> combinedRow = new ArrayList<>(row1);
                combinedRow.addAll(row2);

                // Ensure that the combined row size matches the new schema size
                if (combinedRow.size() != newSchemaSize) {
                    throw new IllegalArgumentException("Row size does not match the relation schema.");
                }

                resultRows.add(combinedRow);
            }
        }

        // Insert the rows into the new relation
        for (List<Cell> resultRow : resultRows) {
            newRelation.insert(resultRow);
        }

        newRelation.print(); // Print the new resulting relation

        return newRelation;
    }// cartesianProduct

    /**
     * Checks whether there are common attributes between two relations, rel1 and
     * rel2.
     * 
     * @param rel1 The first relation.
     * @param rel2 The second relation.
     * @return True if there are common attributes between rel1 and rel2, false
     *         otherwise.
     * 
     */
    private static boolean haveCommonAttributes(Relation rel1, Relation rel2) {
        Set<String> attributes1 = new HashSet<>(rel1.getAttrs());
        Set<String> attributes2 = new HashSet<>(rel2.getAttrs());

        attributes1.retainAll(attributes2); // Intersection of attribute sets

        return !attributes1.isEmpty();

    }// commonAttributes

    /**
     * @inheritDoc
     */
    public Relation project(Relation rel, List<String> attrs) {
        // creates new empty relation to store results
        RelationBuilder newRelBuilder = new RelationBuilder();
        newRelBuilder.attributeNames(attrs);

        List<Type> filteredTypes = attrs.stream()
                .map(attr -> rel.getTypes().get(rel.getAttrs().indexOf(attr)))
                .collect(Collectors.toList());

        newRelBuilder.attributeTypes(filteredTypes);

        Relation newRel = newRelBuilder.build();

        // iterate through each row of the given relation
        for (int i = 0; i < rel.getSize(); i++) {
            List<Cell> originalRow = rel.getRow(i);

            // create a new row with only the specified attributes
            List<Cell> projectedRow = new ArrayList<>();

            for (String attribute : attrs) {
                int index = rel.getAttrs().indexOf(attribute);

                if (index != -1 && index < originalRow.size()) {
                    projectedRow.add(originalRow.get(index));
                } else {
                    System.err.println("Attribute '" + attribute + " ' not found in the original relation.");
                }

            }

            // insert the projected row into the new relation
            newRel.insert(projectedRow);
        }

        // print the new resulting relation
        //newRel.print();

        return newRel;
    }

}
