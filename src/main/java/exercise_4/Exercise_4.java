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
import java.util.List;
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

		//optimal number of iterations and dumping factor
		List<Row> twoAgo = null;
		List<Row> lastIteration = null;
		int maxIterations = 1;

		for (int j = 1; j <= 20; j++) {
			PageRank pRank = gf.pageRank().resetProbability(0.15).maxIter(j);
			Dataset<Row> articles = pRank.run().vertices().select("id", "pagerank");

			Dataset<Row> mostRelevantArticles = articles.sort(org.apache.spark.sql.functions.desc("pagerank"));
			List<Row> thisIteration = mostRelevantArticles.select("id").limit(10).collectAsList();
			System.out.println("Damping factor: " + 0.85 + ", number of iterations: " + j + "\n");
			mostRelevantArticles.show(10);
			if (thisIteration.equals(twoAgo)) {
				maxIterations = j-2;
				break;
			}
			if (thisIteration.equals(lastIteration)) {
				maxIterations = j-1;
				break;
			}
			twoAgo = lastIteration;
			lastIteration = thisIteration;
		}

		System.out.println("Damping factor: " + 0.85 + ", number of iterations: " + maxIterations + "\n");
	}
}
