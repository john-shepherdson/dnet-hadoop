package eu.dnetlib.pace.model

import com.jayway.jsonpath.{Configuration, JsonPath, Option}
import eu.dnetlib.pace.config.{DedupConfig, Type}
import eu.dnetlib.pace.tree.support.TreeProcessor
import eu.dnetlib.pace.util.MapDocumentUtil.truncateValue
import eu.dnetlib.pace.util.{BlockProcessor, MapDocumentUtil, SparkReporter}
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD.rddToPairRDDFunctions
import org.apache.spark.sql.catalyst.encoders.RowEncoder
import org.apache.spark.sql.catalyst.expressions.{GenericRowWithSchema, Literal}
import org.apache.spark.sql.expressions._
import org.apache.spark.sql.functions.{col, lit, udf}
import org.apache.spark.sql.types._
import org.apache.spark.sql.{Column, Dataset, Encoder, Encoders, Row, functions}

import java.util
import java.util.function.Predicate
import java.util.regex.Pattern
import java.util.stream.Collectors
import scala.collection.JavaConverters._
import scala.collection.mutable

case class SparkDedupConfig(conf: DedupConfig, numPartitions: Int) extends Serializable {

  private val URL_REGEX: Pattern = Pattern.compile("^\\s*(http|https|ftp)\\://.*")

  private val CONCAT_REGEX: Pattern = Pattern.compile("\\|\\|\\|")

  private val urlFilter = (s: String) => URL_REGEX.matcher(s).matches

  val modelExtractor: (Dataset[String] => Dataset[Row]) = df => {

    df.withColumn("mapDocument", rowFromJsonUDF.apply(df.col(df.columns(0))))
      .withColumn("identifier", new Column("mapDocument.identifier"))
      //.repartition(new Column("identifier"))
      .dropDuplicates("identifier")
      .select("mapDocument.*")

    df.map(r => rowFromJson(r))(RowEncoder(rowDataType))
      .dropDuplicates("identifier")
  }

  val generateClusters: (Dataset[Row] => Dataset[Row]) = df => {

    val df_with_filters = conf.getPace.getModel.asScala.foldLeft(df)((res, fdef) => {
      if (conf.blacklists.containsKey(fdef.getName)) {
        res.withColumn(
          fdef.getName + "_filtered",
          filterColumnUDF(fdef).apply(new Column(fdef.getName))
        )
      } else {
        res
      }
    })

    val df_with_keys = conf
      .clusterings()
      .asScala
      .foldLeft(df_with_filters)((res, cd) => {
        res.withColumn(
          cd.getName + "_clustered",
          functions.explode_outer(
            clusterValuesUDF(cd).apply(
              functions.array(
                cd.getFields.asScala
                  .map(f => res.col(if (conf.blacklists.containsKey(f)) f.concat("_filtered") else f)): _*
              )
            )
          )
        )
      })

    // filter blacklisted values// filter blacklisted values
    // create one column per cluster prefix// create one column per cluster prefix

    // GROUPING sets approach// GROUPING sets approach
    val tempTable = this.getClass.getSimpleName + "__generateClusters";

    df_with_keys.createOrReplaceTempView(this.getClass.getSimpleName + "__generateClusters")

    val keys = conf.clusterings().asScala.map(_.getName + "_clustered").mkString(",")
    val fields = rowDataType.fieldNames.mkString(",")

    //  Using SQL because GROUPING SETS are not available through Scala/Java DSL
    df_with_keys.sqlContext.sql(
      ("SELECT coalesce(" + keys + ") as key, sort_array(collect_sort_slice(" + fields + ")) as block FROM " + tempTable + " WHERE coalesce(" + keys + ") IS NOT NULL GROUP BY GROUPING SETS (" + keys + ") HAVING size(block) > 1")
    )

  }

  val generateClustersWithDFAPI: (Dataset[Row] => Dataset[Row]) = df => {

   System.out.println(conf.getWf.getEntityType + "::" +conf.getWf.getSubEntityType)

    val df_with_filters = conf.getPace.getModel.asScala.foldLeft(df)((res, fdef) => {
      if (conf.blacklists.containsKey(fdef.getName)) {
        res.withColumn(
          fdef.getName + "_filtered",
          filterColumnUDF(fdef).apply(new Column(fdef.getName))
        )
      } else {
        res
      }
    })

    var relBlocks: Dataset[Row] = null

    import scala.collection.JavaConversions._

    for (cd <- conf.clusterings()) {
      val columns: util.List[Column] = new util.ArrayList[Column](cd.getFields().size)

      for (fName <- cd.getFields()) {
        if (conf.blacklists.containsKey(fName))
          columns.add(new Column(fName + "_filtered"))
        else
          columns.add(new Column(fName))
      }

      val tmp: Dataset[Row] = df_with_filters.withColumn("key", functions.explode(clusterValuesUDF(cd).apply(functions.array(columns.asScala: _*))))




       /*.select((Seq(rowDataType.fieldNames: _*) ++ Seq("key")).map(col): _*)
       .groupByKey(r => r.getAs[String]("key"))(Encoders.STRING)
        .agg(collectSortSliceAggregator.toColumn)
        .toDF("key", "block")
        .select(col("block.block").as("block"))*/

      System.out.println(cd.getName)

      val  ds = tmp.groupBy("key")
      // .agg(functions.sort_array(collectSortSliceUDAF(rowDataType.fieldNames.map(col): _*)).as("block"))
       .agg(functions.collect_set(functions.struct(rowDataType.fieldNames.map(col): _*)).as("block"))
        //.filter(functions.size(new Column("block")).geq(new Literal(2, DataTypes.IntegerType)))


      //df_with_filters.printSchema()
      //ds.printSchema()

      if (relBlocks == null) relBlocks = ds
      else relBlocks = relBlocks.union(ds)
    }

   // System.out.println()

    relBlocks
  }

  val generateClustersWithWindows: (Dataset[Row] => Dataset[Row]) = df => {

    val df_with_filters = conf.getPace.getModel.asScala.foldLeft(df)((res, fdef) => {
      if (conf.blacklists.containsKey(fdef.getName)) {
        res.withColumn(
          fdef.getName + "_filtered",
          filterColumnUDF(fdef).apply(new Column(fdef.getName))
        )
      } else {
        res
      }
    })

    var relBlocks: Dataset[Row] = null

    import scala.collection.JavaConversions._

    for (cd <- conf.clusterings()) {
      System.out.println(conf.getWf.getEntityType + "::" + conf.getWf.getSubEntityType+ ": " + cd.getName + " " + cd.toString)

      val columns: util.List[Column] = new util.ArrayList[Column](cd.getFields().size)

      for (fName <- cd.getFields()) {
        if (conf.blacklists.containsKey(fName))
          columns.add(new Column(fName + "_filtered"))
        else
          columns.add(new Column(fName))
      }

      // Add 'key' column with the value generated by the given clustering definition
      val ds: Dataset[Row] = df_with_filters.withColumn("key", functions.explode(clusterValuesUDF(cd).apply(functions.array(columns.asScala: _*))))
        // Add position column having the position of the row within the set of rows having the same key value ordered by the sorting value
        .withColumn("position", functions.row_number().over(Window.partitionBy("key").orderBy(col(conf.getWf.getOrderField), col("identity"))))
        // filter out rows with position exceeding the maxqueuesize parameter
        .filter(col("position").leq(conf.getWf.getQueueMaxSize))
        .groupBy("key")
        .agg(functions.collect_set(functions.struct(rowDataType.fieldNames.map(col): _*)).as("block"))
        .filter(functions.size(new Column("block")).geq(new Literal(2, DataTypes.IntegerType)))

      if (relBlocks == null) relBlocks = ds
      else relBlocks = relBlocks.union(ds)
    }

    relBlocks
  }

  val generateClustersWithDFAPIMerged: (Dataset[Row] => Dataset[Row]) = df => {
    val df_with_filters = conf.getPace.getModel.asScala.foldLeft(df)((res, fdef) => {
      if (conf.blacklists.containsKey(fdef.getName)) {
        res.withColumn(
          fdef.getName + "_filtered",
          filterColumnUDF(fdef).apply(new Column(fdef.getName))
        )
      } else {
        res
      }
    })

    import scala.collection.JavaConversions._

   val keys = conf.clusterings().foldLeft(null : Column)((res, cd) => {
     val columns: util.List[Column] = new util.ArrayList[Column](cd.getFields().size)

     for (fName <- cd.getFields()) {
       if (conf.blacklists.containsKey(fName))
         columns.add(new Column(fName + "_filtered"))
       else
         columns.add(new Column(fName))
     }

     if (res != null)
     functions.array_union(res, clusterValuesUDF(cd).apply(functions.array(columns.asScala: _*)))
     else
     clusterValuesUDF(cd).apply(functions.array(columns.asScala: _*))
   })

      val ds: Dataset[Row] = df_with_filters.withColumn("key", functions.explode(keys))
        .select((Seq(rowDataType.fieldNames: _*) ++ Seq("key")).map(col): _*)
        .groupByKey(r => r.getAs[String]("key"))(Encoders.STRING)
        .agg(collectSortSliceAggregator.toColumn)
        .toDF("key", "block")
        .select(col("block.block").as("block"))

        /*.groupBy("key")
        .agg(collectSortSliceUDAF(rowDataType.fieldNames.map(col): _*).as("block"))*/
        .filter(functions.size(new Column("block")).geq(new Literal(2, DataTypes.IntegerType)))

    ds
  }

  val generateClustersWithRDDReduction: (Dataset[Row] => Dataset[Row]) = df => {
    val df_with_filters = conf.getPace.getModel.asScala.foldLeft(df)((res, fdef) => {
      if (conf.blacklists.containsKey(fdef.getName)) {
        res.withColumn(
          fdef.getName + "_filtered",
          filterColumnUDF(fdef).apply(new Column(fdef.getName))
        )
      } else {
        res
      }
    })

    var relBlocks: Dataset[Row] = null

    import scala.collection.JavaConversions._

    for (cd <- conf.clusterings()) {
      val columns: util.List[Column] = new util.ArrayList[Column](cd.getFields().size)

      for (fName <- cd.getFields()) {
        if (conf.blacklists.containsKey(fName))
          columns.add(new Column(fName + "_filtered"))
        else
          columns.add(new Column(fName))
      }

      val ds: Dataset[Row] = df.sparkSession.createDataFrame(df_with_filters.withColumn("key", functions.explode(clusterValuesUDF(cd).apply(functions.array(columns.asScala: _*))))
        .select(col("key"), functions.array(functions.struct(rowDataType.fieldNames.map(col): _*).as("value")))
        .rdd.keyBy(_.getString(0))
        .reduceByKey((a, b) => {
          val b1 = a.getSeq[Row](1)
          val b2 = b.getSeq[Row](1)

          if (b1.size + b2.size > conf.getWf.getQueueMaxSize)
            Row(a.get(0), b1.union(b2).sortBy(_.getString(orderingFieldPosition)).slice(0, conf.getWf.getQueueMaxSize))
          else
            Row(a.get(0), b1.union(b2))
        })
        .map(_._2)
        .filter(k => k.getSeq(1).size > 1),
        new StructType().add(StructField("key", DataTypes.StringType)).add(StructField("block", ArrayType(rowDataType)))
      )

      if (relBlocks == null) relBlocks = ds
      else relBlocks = relBlocks.union(ds)
    }

    relBlocks
  }

  val printAnalytics: (Dataset[Row] => Dataset[Row]) = df => {

    val df_with_filters = conf.getPace.getModel.asScala.foldLeft(df)((res, fdef) => {
      if (conf.blacklists.containsKey(fdef.getName)) {
        res.withColumn(
          fdef.getName + "_filtered",
          filterColumnUDF(fdef).apply(new Column(fdef.getName))
        )
      } else {
        res
      }
    })

    var relBlocks: Dataset[Row] = null

    import scala.collection.JavaConversions._

    for (cd <- conf.clusterings()) {
      val columns: util.List[Column] = new util.ArrayList[Column](cd.getFields().size)

      for (fName <- cd.getFields()) {
        if (conf.blacklists.containsKey(fName))
          columns.add(new Column(fName + "_filtered"))
        else
          columns.add(new Column(fName))
      }

      // Add 'key' column with the value generated by the given clustering definition
      val ds: Dataset[Row] = df_with_filters.withColumn("key", functions.explode(clusterValuesUDF(cd).apply(functions.array(columns.asScala: _*))))
        // Add position column having the position of the row within the set of rows having the same key value ordered by the sorting value
        .withColumn("position", functions.row_number().over(Window.partitionBy("key").orderBy(conf.getWf.getOrderField)))
        // filter out rows with position exceeding the maxqueuesize parameter
        .filter(col("position").lt(conf.getWf.getQueueMaxSize))


      // inner join to compute all combination of rows to compare
      // note the condition on position to obtain 'windowing': given a row this is compared at most with the next
      // SlidingWindowSize rows following the sort order
      val dsWithMatch = ds.as("l").join(ds.as("r"),
        col("l.key").equalTo(col("r.key")),
        "inner"
      )
        .filter((col("l.position").lt(col("r.position")))
          && (col("r.position").lt(col("l.position").plus(lit(conf.getWf.getSlidingWindowSize)))))
        // Add match column with the result of comparison
      // dsWithMatch.show(false)

      if (relBlocks == null)
        relBlocks = dsWithMatch
      else
        relBlocks = relBlocks.union(dsWithMatch)
    }

    System.out.println(conf.getWf.getEntityType + "::" + conf.getWf.getSubEntityType)
    System.out.println("Total number of comparations: " + relBlocks.count())


    df
  }


  val generateAndProcessClustersWithJoins: (Dataset[Row] => Dataset[Row]) = df => {

    val df_with_filters = conf.getPace.getModel.asScala.foldLeft(df)((res, fdef) => {
      if (conf.blacklists.containsKey(fdef.getName)) {
        res.withColumn(
          fdef.getName + "_filtered",
          filterColumnUDF(fdef).apply(new Column(fdef.getName))
        )
      } else {
        res
      }
    })

    var relBlocks: Dataset[Row] = null

    import scala.collection.JavaConversions._

    for (cd <- conf.clusterings()) {
      val columns: util.List[Column] = new util.ArrayList[Column](cd.getFields().size)

      for (fName <- cd.getFields()) {
        if (conf.blacklists.containsKey(fName))
          columns.add(new Column(fName + "_filtered"))
        else
          columns.add(new Column(fName))
      }

      // Add 'key' column with the value generated by the given clustering definition
      val ds: Dataset[Row] = df_with_filters.withColumn("key", functions.explode(clusterValuesUDF(cd).apply(functions.array(columns.asScala: _*))))
        // Add position column having the position of the row within the set of rows having the same key value ordered by the sorting value
        .withColumn("position", functions.row_number().over(Window.partitionBy("key").orderBy(conf.getWf.getOrderField)))
        // filter out rows with position exceeding the maxqueuesize parameter
        .filter(col("position").lt(conf.getWf.getQueueMaxSize))


      // inner join to compute all combination of rows to compare
      // note the condition on position to obtain 'windowing': given a row this is compared at most with the next
      // SlidingWindowSize rows following the sort order
      val dsWithMatch = ds.as("l").join(ds.as("r"),
        col("l.key").equalTo(col("r.key")),
        "inner"
        )
        .filter((col("l.position").lt(col("r.position")))
          && (col("r.position").lt(col("l.position").plus(lit(conf.getWf.getSlidingWindowSize)))))
        // Add match column with the result of comparison
        .withColumn("match", udf[Boolean, Row, Row]((a, b) => {
          val treeProcessor = new TreeProcessor(conf)

         treeProcessor.compare(a, b)
        }).apply(functions.struct(rowDataType.fieldNames.map(s => col("l.".concat(s))): _*), functions.struct(rowDataType.fieldNames.map(s => col("r.".concat(s))): _*)))
        .filter(col("match").equalTo(true))
        .select(col("l.identifier").as("from"), col("r.identifier").as("to"))

     // dsWithMatch.show(false)

      if (relBlocks == null)
        relBlocks = dsWithMatch
      else
        relBlocks = relBlocks.union(dsWithMatch)
    }

    val res = relBlocks
      //.select(col("l.identifier").as("from"), col("r.identifier").as("to"))
      //.repartition()
      .distinct()

   // res.show(false)
    res.select(functions.struct("from", "to"))
  }

  val processClusters: (Dataset[Row] => Dataset[Row]) = df => {

    val entity = conf.getWf.getEntityType

    df.filter(functions.size(new Column("block")).geq(new Literal(2, DataTypes.IntegerType)))
      .withColumn("relations", processBlock(df.sqlContext.sparkContext).apply(new Column("block")))
      .select(functions.explode(new Column("relations")).as("relation"))
      //.repartition(new Column("relation"))
      .dropDuplicates("relation")
  }

  val rowDataType: StructType = {
//    val unordered = conf.getPace.getModel.asScala.foldLeft(
//      new StructType()
//    )((resType, fdef) => {
//      resType.add(fdef.getType match {
//        case Type.List | Type.JSON =>
//          StructField(fdef.getName, DataTypes.createArrayType(DataTypes.StringType), true, Metadata.empty)
//        case Type.DoubleArray =>
//          StructField(fdef.getName, DataTypes.createArrayType(DataTypes.DoubleType), true, Metadata.empty)
//        case _ =>
//          StructField(fdef.getName, DataTypes.StringType, true, Metadata.empty)
//      })
//    })
//
//    conf.getPace.getModel.asScala.filterNot(_.getName.equals(conf.getWf.getOrderField)).foldLeft(
//      new StructType()
//        .add(unordered(conf.getWf.getOrderField))
//        .add(StructField("identifier", DataTypes.StringType, false, Metadata.empty))
//    )((resType, fdef) => resType.add(unordered(fdef.getName)))

    val identifier = new FieldDef()
    identifier.setName("identifier")
    identifier.setType(Type.String)

    (conf.getPace.getModel.asScala ++ Seq(identifier)).sortBy(_.getName)
      .foldLeft(
          new StructType()
        )((resType, fdef) => {
          resType.add(fdef.getType match {
            case Type.List | Type.JSON =>
              StructField(fdef.getName, DataTypes.createArrayType(DataTypes.StringType), true, Metadata.empty)
            case Type.DoubleArray =>
              StructField(fdef.getName, DataTypes.createArrayType(DataTypes.DoubleType), true, Metadata.empty)
            case _ =>
              StructField(fdef.getName, DataTypes.StringType, true, Metadata.empty)
          })
        })



  }

  val identityFieldPosition: Int = rowDataType.fieldIndex("identifier")

  val orderingFieldPosition: Int = rowDataType.fieldIndex(conf.getWf.getOrderField)

  def rowFromJson(json: String) : Row = {
    val documentContext =
      JsonPath.using(Configuration.defaultConfiguration.addOptions(Option.SUPPRESS_EXCEPTIONS)).parse(json)
    val values = new Array[Any](rowDataType.size)

    values(identityFieldPosition) = MapDocumentUtil.getJPathString(conf.getWf.getIdPath, documentContext)

    rowDataType.fieldNames.zipWithIndex.foldLeft(values) {
      case ((res, (fname, index))) => {
        val fdef = conf.getPace.getModelMap.get(fname)

        if (fdef != null) {
          res(index) = fdef.getType match {
            case Type.String | Type.Int =>
              MapDocumentUtil.truncateValue(
                MapDocumentUtil.getJPathString(fdef.getPath, documentContext),
                fdef.getLength
              )

            case Type.URL =>
              var uv = MapDocumentUtil.getJPathString(fdef.getPath, documentContext)
              if (!urlFilter(uv)) uv = ""
              uv

            case Type.List | Type.JSON =>
              MapDocumentUtil.truncateList(
                MapDocumentUtil.getJPathList(fdef.getPath, documentContext, fdef.getType),
                fdef.getSize
              ).toArray

            case Type.StringConcat =>
              val jpaths = CONCAT_REGEX.split(fdef.getPath)

              truncateValue(
                jpaths
                  .map(jpath => MapDocumentUtil.getJPathString(jpath, documentContext))
                  .mkString(" "),
                fdef.getLength
              )

            case Type.DoubleArray =>
              MapDocumentUtil.getJPathArray(fdef.getPath, json)
          }
        }

        res
      }
    }

    new GenericRowWithSchema(values, rowDataType)
  }

  val rowFromJsonUDF = udf((json: String) => rowFromJson(json), rowDataType)

  def filterColumnUDF(fdef: FieldDef): UserDefinedFunction = {

    val blacklist: Predicate[String] = conf.blacklists().get(fdef.getName)

    if (blacklist == null) {
      throw new IllegalArgumentException("Column: " + fdef.getName + " does not have any filter")
    } else {
      fdef.getType match {
        case Type.List | Type.JSON =>
          udf[Array[String], Array[String]](values => {
            values.filter((v: String) => !blacklist.test(v))
          })

        case _ =>
          udf[String, String](v => {
            if (blacklist.test(v)) ""
            else v
          })
      }
    }
  }

  def clusterValuesUDF(cd: ClusteringDef) = {
    udf[mutable.WrappedArray[String], mutable.WrappedArray[Object]](values => {
      values.flatMap(f => cd.clusteringFunction().apply(conf, Seq(f.toString).asJava).asScala).map(cd.getName.concat(_))
    })
  }

  def processBlock(implicit sc: SparkContext) = {
    val accumulators = SparkReporter.constructAccumulator(conf, sc)

    udf[Array[Tuple2[String, String]], mutable.WrappedArray[Row]](block => {
      val reporter = new SparkReporter(accumulators)

      val mapDocuments = block.asJava.stream
        .sorted(new RowDataOrderingComparator(orderingFieldPosition, identityFieldPosition))
        .limit(conf.getWf.getQueueMaxSize)
        .collect(Collectors.toList[Row]())

      new BlockProcessor(conf, identityFieldPosition, orderingFieldPosition).processSortedRows(mapDocuments, reporter)

      reporter.getRelations.asScala.toArray
    }).asNondeterministic()
  }

  val collectSortSliceAggregator : Aggregator[Row,Seq[Row], Row] = new Aggregator[Row, Seq[Row], Row] () {
    override def zero: Seq[Row] = Seq[Row]()


    override def reduce(buffer: Seq[Row], input: Row): Seq[Row] = {
      merge(buffer, Seq(input))
    }

    override def merge(buffer: Seq[Row], toMerge: Seq[Row]): Seq[Row] = {
      val newBlock = buffer ++ toMerge

      if (newBlock.size > conf.getWf.getQueueMaxSize)
        newBlock.sortBy(_.getString(orderingFieldPosition)).slice(0, conf.getWf.getQueueMaxSize)
      else
        newBlock
    }

    override def finish(reduction: Seq[Row]): Row = {
      Row(reduction.toArray)
    }

    override def bufferEncoder: Encoder[Seq[Row]] = Encoders.kryo[Seq[Row]]

    override def outputEncoder: Encoder[Row] = RowEncoder.apply(new StructType().add("block", DataTypes.createArrayType(rowDataType), nullable = true))
  }

  val collectSortSliceUDAF : UserDefinedAggregateFunction = new UserDefinedAggregateFunction {
    override def inputSchema: StructType = rowDataType

    override def bufferSchema: StructType = {
      new StructType().add("block", DataTypes.createArrayType(rowDataType), nullable = true)
    }

    override def dataType: DataType = DataTypes.createArrayType(rowDataType)

    override def deterministic: Boolean = true

    override def initialize(buffer: MutableAggregationBuffer): Unit = {
      buffer(0) = Seq[Row]()
    }

    override def update(buffer: MutableAggregationBuffer, input: Row): Unit = {
      val newBlock = buffer.getSeq[Row](0) ++ Seq(input)

      if (newBlock.size > conf.getWf.getQueueMaxSize)
        buffer(0) = newBlock.sortBy(_.getString(orderingFieldPosition)).slice(0, conf.getWf.getQueueMaxSize)
      else
        buffer(0) = newBlock
    }

    override def merge(buffer: MutableAggregationBuffer, row: Row): Unit = {
      val newBlock = buffer.getSeq[Row](0) ++ row.getSeq[Row](0)

      if (newBlock.size > conf.getWf.getQueueMaxSize)
        buffer(0) = newBlock.sortBy(_.getString(orderingFieldPosition)).slice(0, conf.getWf.getQueueMaxSize)
      else
        buffer(0) = newBlock
    }

    override def evaluate(buffer: Row): Any = {
      buffer.getSeq[Row](0)
    }
  }
}
