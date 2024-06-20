package uga.cs4370.mydbimpl;

import java.util.ArrayList;
import java.util.List;
import java.util.HashSet;
import java.util.stream.Collectors;
import java.util.Set;

import uga.cs4370.mydb.Relation;
import uga.cs4370.mydb.RelationBuilder;
import uga.cs4370.mydb.Type;

import uga.cs4370.mydb.Cell;
import uga.cs4370.mydb.RA;

import uga.cs4370.mydb.Predicate;;

public class Driver {

    public static void main(String[] args) {

        // loading relations with table information
        //department
        Relation departmentRelation = new RelationBuilder()
                .attributeNames(List.of("dept_name", "building", "budget"))
                .attributeTypes(List.of(Type.STRING, Type.STRING, Type.DOUBLE))
                .build();
        departmentRelation.loadData("mysql_files/department_export.csv");

        //classroom
        Relation classroomRelation = new RelationBuilder()
                .attributeNames(List.of("building", "room_number", "capacity"))
                .attributeTypes(List.of(Type.STRING, Type.INTEGER, Type.INTEGER))
                .build();
        classroomRelation.loadData("mysql_files/classroom_export.csv");

        //instructor
        Relation instructorRelation = new RelationBuilder()
                .attributeNames(List.of("instructor_id", "instructor_name", "department", "salary"))
                .attributeTypes(List.of(Type.INTEGER, Type.STRING, Type.STRING, Type.DOUBLE))
                .build();
        instructorRelation.loadData("mysql_files/instructor_export.csv");

        //teaches
        Relation teachesRelation = new RelationBuilder()
                .attributeNames(List.of("instructor_id", "course_id", "sec_id", "semester", "year"))
                .attributeTypes(List.of(Type.INTEGER, Type.INTEGER, Type.INTEGER, Type.STRING, Type.INTEGER))
                .build();
        teachesRelation.loadData("mysql_files/teaches_export.csv");

        //course
        Relation courseRelation = new RelationBuilder()
                .attributeNames(List.of("course_id", "title", "dept_name", "credits"))
                .attributeTypes(List.of(Type.INTEGER, Type.STRING, Type.STRING, Type.INTEGER))
                .build();
        courseRelation.loadData("mysql_files/course_export.csv");

        //prereq
        Relation prereqRelation = new RelationBuilder()
            .attributeNames(List.of("course_id", "prereq_id"))
            .attributeTypes(List.of(Type.STRING, Type.STRING))
            .build();
        prereqRelation.loadData("mysql_files/prereq_export.csv");

	//takes
	Relation takesRelation = new RelationBuilder()
                .attributeNames(List.of("ID", "course_id", "dec_id", "semester", "year", "grade"))
                .attributeTypes(List.of(Type.INTEGER, Type.INTEGER, Type.INTEGER, Type.STRING, Type.DOUBLE, Type.STRING))
                .build();
        takesRelation.loadData("mysql_files/takes_export.csv");

        //Query 1: "Classrooms in the Astronomy building that can hold more than 70 students" = 
        // PROJECT building, room_number, capacity (SELECT building=="Taylor" ∧ capacity > 70 (department ⨝ classroom))
        Predicate astronomyPredicate = new Predicate() {
            @Override
            public boolean check(List<Cell> row) {
                String building = row.get(1).toString(); // Building name
                double budget = Double.parseDouble(row.get(2).toString()); // Budget
                return building.equals("Taylor") && budget > 70.0;
            }
        };

        // natural join on the department and classroom relations
        RA ra = new RAimpl();
        Relation joinResult = ra.join(departmentRelation, classroomRelation);

        // apply the predicate to filter rows
        Relation filteredResult = ra.select(joinResult, astronomyPredicate);

        // project required attributes: building, room_number, capacity
        List<String> projectionAttributes = List.of("building", "room_number", "capacity");
        Relation projectedResult = ra.project(filteredResult, projectionAttributes);

        // Print the result
        projectedResult.print();

        //Query 2: "Courses instructor Sakurai teaches" = 
        // PROJECT course_id, title (SELECT instructor.ID=="95709" (instructor ⨝ teaches ⨝ course))
        RA ra2 = new RAimpl();

        // Courses taught by instructor Sakurai
        Predicate predicate = new Predicate() {
            @Override
            public boolean check(List<Cell> row) {
                String instructorId = row.get(0).toString(); 
                return instructorId.equals("95709");
            }
        };
        // perform the selection and join operations
        Relation joinedRelation = ra2.join(instructorRelation, teachesRelation);
        Relation filteredRelation = ra2.select(joinedRelation, predicate);
        Relation finalResult = ra2.join(filteredRelation, courseRelation);
        
        // project course_id and title attributes
        List<String> projectionAttributes2 = List.of("course_id", "title");
        Relation projectedResult2 = ra.project(finalResult, projectionAttributes2);
        
        // print
        projectedResult2.print();

        //Query 3: "Prerequisites for classes in biology department 
        //PROJECT course_id, prereq_id (SELECT dept_name = "Biology"(courseRelation ⋈ prereqRelation))
        RA ra3 = new RAimpl();
        
        // Predicate for filtering prereqs for classes under bio dept
        Predicate predicate3 = new Predicate() {
            @Override
            public boolean check(List<Cell> row) {
                String deptName = row.get(2).toString(); //                 
                return deptName.equals("Biology");
                
            }
        };

       //natural join
        Relation joinResult3 = ra3.join(courseRelation, prereqRelation);

        // apply the predicate to filter rows
        Relation filteredResult3 = ra3.select(joinResult, predicate3);

        // project attributes: semester, year, time
        List<String> projectionAttributes3 = List.of("course_id", "prereq_id");
        Relation projectedResult3 = ra3.project(filteredResult, projectionAttributes);

        // Print the result
        projectedResult3.print();

        //Query 4: Instructors in the Statistics department who don't have a salary > 70,000.0
	// PROJECT instructor.id, instructor.name (SELECT salary <= 70000.0 (Instructor ⨝ (Instructor.dept_name = Department.dept_name) Department))
        RA ra4 = new RAimpl();
        Predicate salaryPredicate = new Predicate() {
            @Override
            public boolean check(List<Cell> row) {
                double salary = Double.parseDouble(row.get(3).toString()); 
                return salary > 70000.0;
            }
        };
        
        // perform  selection on the instructor relation using the salary predicate
        Relation highSalaryInstructors = ra4.select(instructorRelation, salaryPredicate);
        
        Predicate statisticsPredicate = new Predicate() {
            @Override
            public boolean check(List<Cell> row) {
                String department = row.get(2).toString(); 
                return department.equals("Statistics");
            }
        };
        Relation statisticsInstructors = ra4.select(instructorRelation, statisticsPredicate);
        
        // uses set difference to find instructors in the Statistics department who don't have a salary greater than 70,000.0
        Relation filteredInstructors = ra4.diff(statisticsInstructors, highSalaryInstructors);
        
        // project attributes: instructor_id, instructor_name
        List<String> projectionAttributes4 = List.of("instructor_id", "instructor_name");
        Relation projectedResult4 = ra4.project(filteredInstructors, projectionAttributes4);
        
        // print
        projectedResult4.print();


        
	//Query 5: Students who got an A in 2003 in the course_id 748
 	RA ra5 = new RAimpl();
        Predicate gradeYearPredicate = new Predicate() {
            @Override
            public boolean check(List<Cell> row) {
                String grade = row.get(5).toString(); 
                double year = Double.parseDouble(row.get(4).toString()); 
                int courseID = Integer.parseInt(row.get(1).toString()); 
                return grade.equals("A ") && year == 2003 && courseID == 748;
            }
        };

        Relation selectedGrades = ra5.select(takesRelation, gradeYearPredicate);
        
        List<String> projectionAttributes5 = List.of("ID");
        Relation projectedResult5 = ra5.project(selectedGrades, projectionAttributes5);
        
        // print
        projectedResult5.print();



    } // main


 

} // driver
