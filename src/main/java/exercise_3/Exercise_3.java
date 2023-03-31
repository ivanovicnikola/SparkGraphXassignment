package exercise_3;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.graphx.*;
import org.apache.spark.storage.StorageLevel;
import scala.Tuple2;
import scala.collection.Iterator;
import scala.collection.JavaConverters;
import scala.reflect.ClassTag$;
import scala.runtime.AbstractFunction1;
import scala.runtime.AbstractFunction2;
import scala.runtime.AbstractFunction3;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

public class Exercise_3 {

    private static class VProg extends AbstractFunction3<Long,Vertex,Vertex,Vertex> implements Serializable {
        @Override
        public Vertex apply(Long vertexID, Vertex vertexValue, Vertex message) {
            if (message._1 == Integer.MAX_VALUE || vertexValue._1 < message._1){
                return vertexValue;
            } else {
                return message;
            }
        }
    }

    private static class sendMsg extends AbstractFunction1<EdgeTriplet<Vertex,Integer>, Iterator<Tuple2<Object,Vertex>>> implements Serializable {
        @Override
        public Iterator<Tuple2<Object, Vertex>> apply(EdgeTriplet<Vertex, Integer> triplet) {
            Integer weight = triplet.attr();

            if (triplet.srcAttr()._1 == Integer.MAX_VALUE) {
                return JavaConverters.asScalaIteratorConverter(new ArrayList<Tuple2<Object,Vertex>>().iterator()).asScala();
            } else {
                //sum of current weight + new weight and add vertex to list
                List<Long> nodes = new ArrayList<>(triplet.srcAttr()._2);
                nodes.add(triplet.dstId());
                Vertex vertex = new Vertex(triplet.srcAttr()._1 + weight, nodes);
                return JavaConverters.asScalaIteratorConverter(Arrays.asList(new Tuple2<Object,Vertex>(triplet.dstId(), vertex)).iterator()).asScala();
            }
        }
    }

    private static class merge extends AbstractFunction2<Vertex,Vertex,Vertex> implements Serializable {
        @Override
        public Vertex apply(Vertex o, Vertex o2) {
            return null;
        }
    }

    public static void shortestPathsExt(JavaSparkContext ctx) {
        Map<Long, String> labels = ImmutableMap.<Long, String>builder()
                .put(1l, "A")
                .put(2l, "B")
                .put(3l, "C")
                .put(4l, "D")
                .put(5l, "E")
                .put(6l, "F")
                .build();

        List<Long> startList = new ArrayList<>();
        startList.add(1l);

        List<Tuple2<Object,Vertex>> vertices = Lists.newArrayList(
                new Tuple2<Object,Vertex>(1l,new Vertex(0, startList)),
                new Tuple2<Object,Vertex>(2l,new Vertex(Integer.MAX_VALUE)),
                new Tuple2<Object,Vertex>(3l,new Vertex(Integer.MAX_VALUE)),
                new Tuple2<Object,Vertex>(4l,new Vertex(Integer.MAX_VALUE)),
                new Tuple2<Object,Vertex>(5l,new Vertex(Integer.MAX_VALUE)),
                new Tuple2<Object,Vertex>(6l,new Vertex(Integer.MAX_VALUE))
        );
        List<Edge<Integer>> edges = Lists.newArrayList(
                new Edge<Integer>(1l,2l, 4), // A --> B (4)
                new Edge<Integer>(1l,3l, 2), // A --> C (2)
                new Edge<Integer>(2l,3l, 5), // B --> C (5)
                new Edge<Integer>(2l,4l, 10), // B --> D (10)
                new Edge<Integer>(3l,5l, 3), // C --> E (3)
                new Edge<Integer>(5l, 4l, 4), // E --> D (4)
                new Edge<Integer>(4l, 6l, 11) // D --> F (11)
        );

        JavaRDD<Tuple2<Object,Vertex>> verticesRDD = ctx.parallelize(vertices);
        JavaRDD<Edge<Integer>> edgesRDD = ctx.parallelize(edges);

        Graph<Vertex,Integer> G = Graph.apply(verticesRDD.rdd(),edgesRDD.rdd(),new Vertex(1), StorageLevel.MEMORY_ONLY(), StorageLevel.MEMORY_ONLY(),
                scala.reflect.ClassTag$.MODULE$.apply(Vertex.class),scala.reflect.ClassTag$.MODULE$.apply(Integer.class));

        GraphOps ops = new GraphOps(G, scala.reflect.ClassTag$.MODULE$.apply(Vertex.class),scala.reflect.ClassTag$.MODULE$.apply(Integer.class));

        ops.pregel(new Vertex(Integer.MAX_VALUE),
                        Integer.MAX_VALUE,
                        EdgeDirection.Out(),
                        new Exercise_3.VProg(),
                        new Exercise_3.sendMsg(),
                        new Exercise_3.merge(),
                        ClassTag$.MODULE$.apply(Vertex.class))
                .vertices()
                .toJavaRDD()
                .foreach(v -> {
                    Tuple2<Object,Vertex> vertex = (Tuple2<Object,Vertex>)v;
                    String result = "Minimum cost to get from "+labels.get(1l)+" to "+ labels.get(vertex._1)+" is [";
                    for (int i=0;i<vertex._2._2.size();i++) {
                        if (i!=0) result+=",";
                        result+=labels.get(vertex._2._2.get(i));
                    }
                    result += "] with cost "+vertex._2._1;
                    System.out.println(result);
                });
    }
	
}
