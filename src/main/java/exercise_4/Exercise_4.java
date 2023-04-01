package exercise_4;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.MetadataBuilder;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.graphframes.GraphFrame;
import org.graphframes.lib.PageRank;

import java.io.*;
import java.util.ArrayList;
import java.util.Scanner;

public class Exercise_4 {
	
	public static void wikipedia(JavaSparkContext ctx, SQLContext sqlCtx) {
		java.util.List<Row> vertices_list = new ArrayList<Row>();
		File vertices_file = new File("C:\\Users\\Bogdana\\Desktop\\UPC\\Semantic_Data_Management\\SparkGraphXassignment\\src\\main\\resources\\wiki-vertices.txt");

		Scanner myReaderVertices = null;
		try {
			myReaderVertices = new Scanner(vertices_file);
		} catch (FileNotFoundException e) {
			throw new RuntimeException(e);
		}
		while (myReaderVertices.hasNextLine()) {
			String data = myReaderVertices.nextLine();
			String[] words = data.split("\\s+", 2);
			vertices_list.add(RowFactory.create(words[0], words[1]));
		}

		JavaRDD<Row> vertices_rdd = ctx.parallelize(vertices_list);

		StructType vertices_schema = new StructType(new StructField[]{
				new StructField("id", DataTypes.StringType, true, new MetadataBuilder().build()),
				new StructField("name", DataTypes.StringType, true, new MetadataBuilder().build())
		});

		Dataset<Row> vertices =  sqlCtx.createDataFrame(vertices_rdd, vertices_schema);

		java.util.List<Row> edges_list = new ArrayList<Row>();

		File edges_file = new File("C:\\Users\\Bogdana\\Desktop\\UPC\\Semantic_Data_Management\\SparkGraphXassignment\\src\\main\\resources\\wiki-edges.txt");

		Scanner myReaderEdges = null;
		try {
			myReaderEdges = new Scanner(edges_file);
		} catch (FileNotFoundException e) {
			throw new RuntimeException(e);
		}
		while (myReaderEdges.hasNextLine()) {
			String data = myReaderEdges.nextLine();
			String[] words = data.split("\\s+", 2);
			edges_list.add(RowFactory.create(words[0], words[1]));
		}

		JavaRDD<Row> edges_rdd = ctx.parallelize(edges_list);

		StructType edges_schema = new StructType(new StructField[]{
				new StructField("src", DataTypes.StringType, true, new MetadataBuilder().build()),
				new StructField("dst", DataTypes.StringType, true, new MetadataBuilder().build())
		});

		Dataset<Row> edges = sqlCtx.createDataFrame(edges_rdd, edges_schema);

		GraphFrame gf = GraphFrame.apply(vertices, edges);

		System.out.println(gf);

		gf.edges().show();
		gf.vertices().show();

		PageRank pRank = gf.pageRank().resetProbability(0.01).maxIter(5);
		Dataset<Row> articles = pRank.run().vertices().select("id", "pagerank");

		Dataset<Row> mostRelevantArticles = articles.sort(org.apache.spark.sql.functions.desc("pagerank"));
		mostRelevantArticles.show(10);
	}
}
