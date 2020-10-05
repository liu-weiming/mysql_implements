import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.stream.Collectors;

public class Test1 {

    public static class Table1 {
        int a;

        public int getA() {
            return a;
        }

        public void setA(int a) {
            this.a = a;
        }

        public Table1(int a) {
            this.a = a;
        }

        @Override
        public String toString() {
            return "Table1{" +
                    "a=" + a +
                    '}';
        }

        public static Table1 build(int a) {
            return new Table1(a);
        }
    }

    public static class Table2 {
        int b;

        public int getB() {
            return b;
        }

        public void setB(int b) {
            this.b = b;
        }

        public Table2(int b) {
            this.b = b;
        }

        public static Table2 build(int b) {
            return new Table2(b);
        }

        @Override
        public String toString() {
            return "Table2{" +
                    "b=" + b +
                    '}';
        }
    }

    public static class Record<R1, R2> {
        R1 r1;
        R2 r2;

        public R1 getR1() {
            return r1;
        }

        public void setR1(R1 r1) {
            this.r1 = r1;
        }

        public R2 getR2() {
            return r2;
        }

        public void setR2(R2 r2) {
            this.r2 = r2;
        }

        public Record(R1 r1, R2 r2) {
            this.r1 = r1;
            this.r2 = r2;
        }

        @Override
        public String toString() {
            return "Record{" +
                    "r1=" + r1 +
                    ", r2=" + r2 +
                    '}';
        }

        public static <R1, R2> Record<R1, R2> build(R1 r1, R2 r2) {
            return new Record<>(r1, r2);
        }
    }

    public static enum JoinType {
        innerJoin, leftJoin
    }

    public static interface Filter<R1, R2> {
        boolean accept(R1 r1, R2 r2);
    }

    public static <R1, R2> List<Record<R1, R2>> join(List<R1> table1, List<R2> table2, JoinType joinType, Filter<R1, R2> onFilter, Filter<R1, R2> whereFilter) {
        if(Objects.isNull(table1) || Objects.isNull(table2) || joinType == null) {
            return new ArrayList<>();
        }

        List<Record<R1, R2>> result = new CopyOnWriteArrayList<>();

        //笛卡尔积
        for(R1 r1 : table1) {
            List<Record<R1, R2>> onceJoinResult = joinOn(r1, table2, onFilter);
            result.addAll(onceJoinResult);
        }

        if(joinType == JoinType.leftJoin) {
            List<R1> r1Record = result.stream().map(Record::getR1).collect(Collectors.toList());
            List<Record<R1, R2>> leftAppendList = new ArrayList<>();
            for (R1 r1 : table1) {
                if (!r1Record.contains(r1)) {
                    leftAppendList.add(Record.build(r1, null));
                }
            }
            result.addAll(leftAppendList);
        }
        if (Objects.nonNull(whereFilter)) {
            for (Record<R1, R2> record : result) {
                if (!whereFilter.accept(record.r1, record.r2)) {
                    result.remove(record);
                }
            }
        }
        return result;
    }

    public static <R1, R2> List<Record<R1, R2>> joinOn(R1 r1, List<R2> table2, Filter<R1, R2> onFilter) {
        List<Record<R1, R2>> result = new ArrayList<>();
        for (R2 r2 : table2) {
            if (Objects.nonNull(onFilter) ? onFilter.accept(r1, r2) : true) {
                result.add(Record.build(r1, r2));
            }
        }
        return result;
    }

    @Test
    public void innerJoin () {
        List<Table1> table1 = Arrays.asList(Table1.build(1), Table1.build(2), Table1.build(3));
        List<Table2> table2 = Arrays.asList(Table2.build(3), Table2.build(4), Table2.build(5));

        join(table1, table2, JoinType.innerJoin, null, null).forEach(System.out::println);
        System.out.println("------------------");
        join(table1, table2, JoinType.innerJoin, (r1, r2) -> r1.a == r2.b, null).forEach(System.out::println);
    }

    @Test
    public void leftJoin() {
        List<Table1> table1 = Arrays.asList(Table1.build(1), Table1.build(2), Table1.build(3));
        List<Table2> table2 = Arrays.asList(Table2.build(3), Table2.build(4), Table2.build(5));

        join(table1, table2, JoinType.leftJoin, null, null).forEach(System.out::println);
        System.out.println("------------------");
        join(table1, table2, JoinType.leftJoin, (r1, r2) -> r1.a > 10, null).forEach(System.out::println);
    }

}
