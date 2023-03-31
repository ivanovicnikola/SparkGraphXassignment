package exercise_3;

import scala.Tuple2;

import java.util.ArrayList;
import java.util.List;

public class Vertex extends Tuple2<Integer, List<String>> {
    public Vertex(Integer _1) {
        super(_1, new ArrayList<>());
    }
}
