import edu.uta.diablo._
import org.apache.spark._
import org.apache.spark.{Partitioner, SparkException}
import org.apache.spark.rdd._
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions.Aggregator
import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder
import org.apache.spark.mllib.linalg.distributed._
import org.apache.spark.mllib.linalg._
import org.apache.log4j._
import org.apache.hadoop.fs._
import scala.collection.Seq
import scala.collection.mutable.HashMap
import scala.util.Random
import Math._

object Join {
  /* The size of an object */
  def sizeof ( x: AnyRef ): Long = {
    import org.apache.spark.util.SizeEstimator.estimate
    estimate(x)
   }

  def main ( args: Array[String] ) {
    val func = args(0).toInt
    val repeats = args(1).toInt
    val n = args(2).toInt
    val m = if (args.length > 3) args(3).toInt else n
    val sparsity = 0.01
    parami(block_dim_size,1000)  // size of each dimension in a block
    val N = 1000
    val M = 1000
    parami(number_of_partitions,10)
    //parami(broadcast_limit, 1000)

    val conf = new SparkConf().setAppName("diablojoin")
    spark_context = new SparkContext(conf)
    val spark = SparkSession.builder().config(conf).getOrCreate()
    import spark.implicits._

    conf.set("spark.logConf","false")
    conf.set("spark.eventLog.enabled","false")
    LogManager.getRootLogger().setLevel(Level.WARN)

    def random () = {
      val rand = new Random()
      rand.nextDouble()*10
    }

    def randomTile ( nd: Int, md: Int ): DenseMatrix = {
      val rand = new Random()
      val max = 10
      new DenseMatrix(nd,md,Array.tabulate(nd*md){ i => rand.nextDouble()*max })
    }

    def randomMatrix ( rows: Int, cols: Int, rdim: Int, cdim: Int ): RDD[((Int, Int),org.apache.spark.mllib.linalg.Matrix)] = {
      val l = Random.shuffle((0 until (rows+rdim-1)/rdim).toList)
      val r = Random.shuffle((0 until (cols+cdim-1)/cdim).toList)
      spark_context.parallelize(for { i <- l; j <- r } yield (i,j),number_of_partitions)
                   .map{ case (i,j) => ((i,j),randomTile(if ((i+1)*rdim > rows) rows%rdim else rdim,
                                                         if ((j+1)*cdim > cols) cols%cdim else cdim)) }
    }

    def randomTileSparse ( nd: Int, md: Int ): SparseMatrix = {
      val max = 10
      val rand = new Random()
      var entries = scala.collection.mutable.ArrayBuffer[(Int,Int,Double)]()
      for (i <- 0 to nd-1; j <- 0 to md-1) {
        if (rand.nextDouble() <= sparsity)
          entries += ((i,j,rand.nextDouble()*max))
      }
      SparseMatrix.fromCOO(nd,md,entries)
    }

    def randomSparseMatrix ( rows: Int, cols: Int ): RDD[((Int, Int),org.apache.spark.mllib.linalg.Matrix)] = {
      val l = Random.shuffle((0 until (rows+N-1)/N).toList)
      val r = Random.shuffle((0 until (cols+N-1)/N).toList)
      spark_context.parallelize(for { i <- l; j <- r } yield (i,j),number_of_partitions)
            .map{ case (i,j) => ((i,j),randomTileSparse(if ((i+1)*N > rows) rows%N else N,
                              if ((j+1)*N > cols) cols%N else N)) }
    }

    val Am = randomMatrix(n,m,N,N).cache()
    val Bm = randomMatrix(m,n,N,N).cache()
    val Cm = randomMatrix(n,100000,N,M).cache()
    val Xm = randomMatrix(n,m,100,100).cache()
    val Ym = randomMatrix(m,n,100,100).cache()
    val Zm = randomMatrix(n,100000,100,100).cache()
    var A = new BlockMatrix(Am,N,N).cache
    val B = new BlockMatrix(Bm,N,N).cache

    type tiled_matrix = ((Int,Int),EmptyTuple,RDD[((Int,Int),((Int,Int),EmptyTuple,Array[Double]))])
    val et = EmptyTuple()
    //  block tensors
    val AA = ((n,m),et,Am.map{ case ((i,j),a) => ((i,j),((a.numRows,a.numCols),et,a.transpose.toArray)) })
    val BB = ((m,n),et,Bm.map{ case ((i,j),a) => ((i,j),((a.numRows,a.numCols),et,a.transpose.toArray)) })
    AA._3.cache
    BB._3.cache

    // sparse block tensors with 99% zeros
    /*val Az = q("tensor*(n)(m)[ ((i,j),random()) | i <- 0..(n-1), j <- 0..(m-1), random() > (1-sparsity)*10 ]")
    Az._3.cache
    val Bz = q("tensor*(n)(m)[ ((i,j),random()) | i <- 0..(n-1), j <- 0..(m-1), random() > (1-sparsity)*10 ]")
    Bz._3.cache*/

    // matrix multiplication of dense matrix and dense vector
    /*def testMultiply (): Double = {
      param(groupByJoin,true)
      val t = System.currentTimeMillis()
      try {
        val C = q("""
                    tensor*(n,n)[ ((i,j),+/c) | ((i,k),a) <- AA, ((kk,j),b) <- BB, k == kk, let c = a*b, group by (i,j) ]
                    """)
        println(C._3.count())
      } catch { case x: Throwable => println(x); return -1.0 }
      param(groupByJoin,false)
      (System.currentTimeMillis()-t)/1000.0
    }*/

    /*def testMultiply (): Double = {
      val t = System.currentTimeMillis()
      try {
          val C = q("""
                    tensor*(n,n)[ ((i,j),a+b) | ((i,j),a) <- AA, ((ii,jj),b) <- BB, i==ii, j==jj ]
                    """)
        println(C._3.count())
      } catch { case x: Throwable => println(x); return -1.0 }
      (System.currentTimeMillis()-t)/1000.0
    }*/

    def block_mult(a: Matrix, b: Matrix): Matrix = a.multiply(b.asInstanceOf[DenseMatrix])

    /*def testMultiply (): Double = {
      val t = System.currentTimeMillis()
      try {
        val C = q("""
                  tensor*(n,n)[ ((i,j),+/c) | ((i,k),a) <- AA, ((kk,j),b) <- BB, k == kk, let c = block_mult(a,b), group by (i,j) ]
                  """)
        println(C._3.count())
        C._3.blocks.collect.map(i => i._2.toArray.map(println))
      } catch { case x: Throwable => println(x); return -1.0 }
      (System.currentTimeMillis()-t)/1000.0
    }

    // matrix multiplication of  matrix and  vector
    def testMultipyDiabloJoin (): Double = {
      val t = System.currentTimeMillis()
      try {
        val C = q("""tensor*(n,p)[ ((i,j),+/c) | ((i,k),a) <- AA, ((kk,j),b) <- BB, k == kk, let c = a*b, group by (i,j) ]""")
        println("C:")
        println(C._3.count())
        C._3.collect.map(i => i._2._3.map(println))
        val A = new BlockMatrix(Am,N,N).cache
        val B = new BlockMatrix(Bm,N,N).cache
        val C1 = A.multiply(B)
        println("MLLib:")
        C1.blocks.collect.map(i => i._2.toArray.map(println))
      } catch { case x: Throwable => println(x); return -1.0 }
      (System.currentTimeMillis()-t)/1000.0
    }*/

    // diablo matrix multiplication of 3  matrices
    /*def testMultipyDiablo (): Double = {
      param(groupByJoin,true)
      val t = System.currentTimeMillis()
      try {
        val C = q("""
          tensor*(n,m)[ ((i,j),+/c) | ((i,k),d) <- tensor*(n,n)[ ((ii,jj),+/c) | ((ii,l),a) <- AA, ((ll,jj),b) <- BB, l == ll, let c = a*b, group by (ii,jj) ], ((kk,j),a) <- AA, k == kk, let c = d*a, group by (i,j) ];
          """)
        println(C._3.count())
        //C._3.collect.map(i => i._2._3.map(println))
      } catch { case x: Throwable => println(x); return -1.0 }
      param(groupByJoin,false)
      (System.currentTimeMillis()-t)/1000.0
    }

    // diablo matrix multiplication of 3  matrices
    def testMultipyDiablo1 (): Double = {
      param(groupByJoin,true)
      val t = System.currentTimeMillis()
      try {
        val C = q("""
          tensor*(n,m)[ ((i,j),+/c) | ((i,k),a) <- AA, ((kk,l),b) <- BB, k == kk, ((ll,j),d) <- AA, l == ll, let c = a*b*d, group by (i,j) ];
          """)
        println(C._3.count())
        //C._3.collect.map(i => i._2._3.map(println))
      } catch { case x: Throwable => println(x); return -1.0 }
      param(groupByJoin,false)
      (System.currentTimeMillis()-t)/1000.0
    }*/

    // mllib matrix multiplication of 3  matrices
    def testMultipyMLlib (): Double = {
      val t = System.currentTimeMillis()
      try {
        val C = A.multiply(B)
        println(C.blocks.count())
        //C1.blocks.collect.map(i => i._2.toArray.map(println))
      } catch { case x: Throwable => println(x); return -1.0 }
      (System.currentTimeMillis()-t)/1000.0
    }

    def transposeBlock(mat1: Matrix): Matrix = {
      val cols = mat1.numCols
      val rows = mat1.numRows
      var arr = Array.ofDim[Double](cols*rows)
      val arr1 = mat1.toArray
      for(i <- (0 to cols-1).par) {
          for(j <- 0 to rows-1) {
              arr(i*rows+j) = arr1(j*cols+i)
          }
      }
      new DenseMatrix(cols,rows,arr)
    }

    def sumBlocks(mat1: Matrix, mat2: Matrix, f: (Double, Double) => Double): DenseMatrix = {
      val cols = mat1.numCols
      val rows = mat1.numRows
      val cols2 = mat2.numCols
      val rows2 = mat2.numRows
      //println("Add Mat1: ("+rows+", "+cols+"), Mat2: ("+rows2+", "+cols2+")")
      var arr = Array.ofDim[Double](rows*cols)
      val arr1 = mat1.toArray
      val arr2 = mat2.toArray
      for(i <- (0 to rows-1).par) {
        for(j <- 0 to cols-1) {
          arr(i*cols+j) = f(arr1(i*cols+j),arr2(i*cols+j))
        }
      }
      new DenseMatrix(rows,cols,arr)
    }

    def multBlocks(mat1: Matrix, mat2: Matrix): Matrix = {
      val cols1 = mat1.numCols
      val rows1 = mat1.numRows
      val cols2 = mat2.numCols
      val rows2 = mat2.numRows
      //println("Multiply Mat1: ("+rows1+", "+cols1+"), Mat2: ("+rows2+", "+cols2+")")
      var arr = Array.ofDim[Double](rows1*cols2)
      val arr1 = mat1.toArray
      val arr2 = mat2.toArray
      for(i <- (0 to rows1-1).par) {
        var j = 0
        while(j < cols2) {
          var k = 0
          while(k < cols1) {
              arr(i*cols2+j) += arr1(i*cols1+k)*arr2(k*cols2+j)
              k+=1
          }
          j+=1
        }
      }
      new DenseMatrix(rows1,cols2,arr)
    }

    // hand written matrix multiplication of 3  matrices
    /*def testMultipyHandWritten (): Double = {
      val t = System.currentTimeMillis()
      try {
        val C = Am.map{case ((i,j),a) => (j,(i,a))}
                  .join(Bm.map{case ((j,k),b) => (j, (k,b))})
                  .map{case (j,((i,a),(k,b))) => ((i,k), multBlocks(transposeBlock(a),transposeBlock(b)))}
                  .reduceByKey{case (a,b) => sumBlocks(a,b,(g,h)=>g+h)}
                  .map{case ((i,j),a) => (j,(i,a))}
                  .join(Am.map{case ((j,k),b) => (j, (k,b))})
                  .map{case (j,((i,a),(k,b))) => ((i,k), multBlocks(transposeBlock(a),transposeBlock(b)))}
                  .reduceByKey{case (a,b) => sumBlocks(a,b,(g,h)=>g+h)}
        println(C.count())
        //C.collect.map(i => i._2.toArray.map(println))
      } catch { case x: Throwable => println(x); return -1.0 }
      (System.currentTimeMillis()-t)/1000.0
    }*/

    // hand written matrix multiplication of 3  matrices
    def testMultipyHandWritten (): Double = {
      val t = System.currentTimeMillis()
      try {
        val C = Am.map{case ((i,j),a) => (j,(i,a))}
                  .groupByKey()
                  .join(Bm.map{case ((j,k),b) => (j, (k,b))}.groupByKey())
                  .flatMap{case (j,(a,b)) => {
                    var m_list: List[((Int, Int), Matrix)] = List()
                    for((i,c) <- a) {
                      for((k,d) <- b) {
                        m_list :+= ((i,k), block_mult(c,d))
                      }
                    }
                    m_list
                  }}
                  .reduceByKey{case (a,b) => sumBlocks(a,b,(g,h)=>g+h)}
                  .map{case ((i,j),a) => (j,(i,a))}.groupByKey()
                  .join(Am.map{case ((j,k),b) => (j, (k,b))}.groupByKey())
                  .flatMap{case (j,(a,b)) => {
                    var m_list: List[((Int, Int), Matrix)] = List()
                    for((i,c) <- a) {
                      for((k,d) <- b) {
                        m_list :+= ((i,k), block_mult(c,d))
                      }
                    }
                    m_list
                  }}
                  .reduceByKey{case (a,b) => sumBlocks(a,b,(g,h)=>g+h)}
        println(C.count())
        //C.collect.map(i => i._2.toArray.map(println))
      } catch { case x: Throwable => println(x); return -1.0 }
      (System.currentTimeMillis()-t)/1000.0
    }

    /*def testMultipyHandWritten (): Double = {
      val t = System.currentTimeMillis()
      try {
        val C = Am.map{case ((i,j),a) => (j,(i,a))}
                  .join(Bm.map{case ((j,k),b) => (j, (k,b))})
                  //.map{case (j,((i,a),(k,b))) => ((i,k), multBlocks(transposeBlock(a),transposeBlock(b)))}
                  .map{case (j,((i,a),(k,b))) => ((i,k), a.multiply(b.asInstanceOf[SparseMatrix].toDense))}
                  .reduceByKey{case (a,b) => sumBlocks(a,b,(g,h)=>g+h)}
                  .map{case ((i,j),a) => (j,(i,a))}
                  .join(Am.map{case ((j,k),b) => (j, (k,b))})
                  //.map{case (j,((i,a),(k,b))) => ((i,k), multBlocks(transposeBlock(a),transposeBlock(b)))}
                  .map{case (j,((i,a),(k,b))) => ((i,k), a.multiply(b.asInstanceOf[SparseMatrix].toDense))}
                  .reduceByKey{case (a,b) => sumBlocks(a,b,(g,h)=>g+h)}
        println(C.count())
        //C.collect.map(i => i._2.toArray.map(println))
      } catch { case x: Throwable => println(x); return -1.0 }
      (System.currentTimeMillis()-t)/1000.0
    }*/

    class PartitionIdPassthrough(override val numPartitions: Int) extends Partitioner {
      override def getPartition(key: Any): Int = key.asInstanceOf[Int]
    }

    class GridPartitioner(
        val rows: Int,
        val cols: Int,
        val rowsPerPart: Int,
        val colsPerPart: Int) extends Partitioner {

      require(rows > 0)
      require(cols > 0)
      require(rowsPerPart > 0)
      require(colsPerPart > 0)

      private val rowPartitions = math.ceil(rows * 1.0 / rowsPerPart).toInt
      private val colPartitions = math.ceil(cols * 1.0 / colsPerPart).toInt

      override val numPartitions: Int = rowPartitions * colPartitions

      override def getPartition(key: Any): Int = {
        key match {
          case i: Int => i
          case (i: Int, j: Int) =>
            getPartitionId(i, j)
          case (i: Int, j: Int, _: Int) =>
            getPartitionId(i, j)
          case _ =>
            throw new IllegalArgumentException(s"Unrecognized key: $key.")
        }
      }

      /** Partitions sub-matrices as blocks with neighboring sub-matrices. */
      private def getPartitionId(i: Int, j: Int): Int = {
        require(0 <= i && i < rows, s"Row index $i out of range [0, $rows).")
        require(0 <= j && j < cols, s"Column index $j out of range [0, $cols).")
        i / rowsPerPart + j / colsPerPart * rowPartitions
      }

      override def equals(obj: Any): Boolean = {
        obj match {
          case r: GridPartitioner =>
            (this.rows == r.rows) && (this.cols == r.cols) &&
              (this.rowsPerPart == r.rowsPerPart) && (this.colsPerPart == r.colsPerPart)
          case _ =>
            false
        }
      }

      override def hashCode: Int = {
        com.google.common.base.Objects.hashCode(
          rows: java.lang.Integer,
          cols: java.lang.Integer,
          rowsPerPart: java.lang.Integer,
          colsPerPart: java.lang.Integer)
      }
    }

    object GridPartitioner {

      /** Creates a new [[GridPartitioner]] instance. */
      def apply(rows: Int, cols: Int, rowsPerPart: Int, colsPerPart: Int): GridPartitioner = {
        new GridPartitioner(rows, cols, rowsPerPart, colsPerPart)
      }

      /** Creates a new [[GridPartitioner]] instance with the input suggested number of partitions. */
      def apply(rows: Int, cols: Int, suggestedNumPartitions: Int): GridPartitioner = {
        require(suggestedNumPartitions > 0)
        val scale = 1.0 / math.sqrt(suggestedNumPartitions)
        val rowsPerPart = math.round(math.max(scale * rows, 1.0)).toInt
        val colsPerPart = math.round(math.max(scale * cols, 1.0)).toInt
        new GridPartitioner(rows, cols, rowsPerPart, colsPerPart)
      }
    }

    type BlockDestinations = Map[(Int, Int), Set[Int]]

    def simulateMultiply(partitioner: GridPartitioner): (BlockDestinations, BlockDestinations) = {
      val A_blockInfo = A.blocks.mapValues(block => (block.numRows, block.numCols)).cache()
      val B_blockInfo = B.blocks.mapValues(block => (block.numRows, block.numCols)).cache()
      val leftMatrix = A_blockInfo.keys.collect()
      val rightMatrix = B_blockInfo.keys.collect()
      val midDimSplitNum = 1

      val rightCounterpartsHelper = rightMatrix.groupBy(_._1).mapValues(_.map(_._2))
      val leftDestinations = leftMatrix.map { case (rowIndex, colIndex) =>
        val rightCounterparts = rightCounterpartsHelper.getOrElse(colIndex, Array.emptyIntArray)
        val partitions = rightCounterparts.map(b => partitioner.getPartition((rowIndex, b)))
        val midDimSplitIndex = colIndex % midDimSplitNum
        ((rowIndex, colIndex),
          partitions.toSet.map((pid: Int) => pid * midDimSplitNum + midDimSplitIndex))
      }.toMap

      val leftCounterpartsHelper = leftMatrix.groupBy(_._2).mapValues(_.map(_._1))
      val rightDestinations = rightMatrix.map { case (rowIndex, colIndex) =>
        val leftCounterparts = leftCounterpartsHelper.getOrElse(rowIndex, Array.emptyIntArray)
        val partitions = leftCounterparts.map(b => partitioner.getPartition((b, colIndex)))
        val midDimSplitIndex = rowIndex % midDimSplitNum
        ((rowIndex, colIndex),
          partitions.toSet.map((pid: Int) => pid * midDimSplitNum + midDimSplitIndex))
      }.toMap

      (leftDestinations, rightDestinations)
    }

    def block_sum(mat1: DenseMatrix, mat2: DenseMatrix): DenseMatrix = {
      val cols = mat1.numCols
      val rows = mat1.numRows
      val cols2 = mat2.numCols
      val rows2 = mat2.numRows
      //println("Add Mat1: ("+rows+", "+cols+"), Mat2: ("+rows2+", "+cols2+")")
      var arr = Array.ofDim[Double](rows*cols)
      val arr1 = mat1.toArray
      val arr2 = mat2.toArray
      for(i <- (0 to rows-1).par) {
        for(j <- 0 to cols-1) {
          arr(i*cols+j) = arr1(i*cols+j)+arr2(i*cols+j)
        }
      }
      new DenseMatrix(rows,cols,arr)
    }

    def testMultipyHandWritten_1 (): Double = {
      val t = System.currentTimeMillis()
      try {
        val resultPartitioner = GridPartitioner(A.numRowBlocks, B.numColBlocks,
          math.max(A.blocks.partitions.length, B.blocks.partitions.length))
        val (leftDestinations, rightDestinations)
          = simulateMultiply(resultPartitioner)
        // Each block of A must be multiplied with the corresponding blocks in the columns of B.
        val flatA = A.blocks.flatMap { case ((blockRowIndex, blockColIndex), block) =>
          val destinations = leftDestinations.getOrElse((blockRowIndex, blockColIndex), Set.empty)
          destinations.map(j => (j, (blockRowIndex, blockColIndex, block)))
        }
        // Each block of B must be multiplied with the corresponding blocks in each row of A.
        val flatB = B.blocks.flatMap { case ((blockRowIndex, blockColIndex), block) =>
          val destinations = rightDestinations.getOrElse((blockRowIndex, blockColIndex), Set.empty)
          destinations.map(j => (j, (blockRowIndex, blockColIndex, block)))
        }
        val intermediatePartitioner = new PartitionIdPassthrough(resultPartitioner.numPartitions)
        val newBlocks_1 = flatA.cogroup(flatB, intermediatePartitioner).flatMap { case (pId, (a, b)) =>
          a.flatMap { case (leftRowIndex, leftColIndex, leftBlock) =>
            b.filter(_._1 == leftColIndex).map { case (rightRowIndex, rightColIndex, rightBlock) =>
              val C = rightBlock match {
                case dense: DenseMatrix => leftBlock.multiply(dense)
                case sparse: SparseMatrix => leftBlock.multiply(sparse.toDense)
                case _ =>
                  throw new SparkException(s"Unrecognized matrix type ${rightBlock.getClass}.")
              }
              ((leftRowIndex, rightColIndex), C)
            }
          }
        }
        val newBlocks = newBlocks_1.reduceByKey(resultPartitioner, (a, b) => block_sum(a,b))
        .mapValues(v => Matrices.dense(v.numRows, v.numCols, v.values))
        val C = new BlockMatrix(newBlocks, A.rowsPerBlock, B.colsPerBlock, A.numRows(), B.numCols())
        println(C.blocks.count())
      } catch { case x: Throwable => println(x); return -1.0 }
      (System.currentTimeMillis()-t)/1000.0
    }

    // hand written matrix multiplication of 3  matrices
    def testMultipyHandWritten1 (): Double = {
      val t = System.currentTimeMillis()
      try {
        val C = Am.map{case ((i,j),a) => (j,(i,a))}
                  .cogroup(Bm.map{case ((j,k),b) => (j, (k,b))})
                  .flatMap{case (j,(a,b)) => {
                    var x: List[((Int,Int),Matrix)] = List()
                    for((i,c) <- a) {
                      for((k,d) <- b) {
                        x :+= ((i,k), multBlocks(c,d))
                      }
                    }
                    x
                  }}
                  .map{case ((i,j),d) => (j,(i,d))}
                  .cogroup(Cm.map{case ((j,k),a) => (j, (k,a))})
                  .flatMap{case (j,(a,b)) => {
                    var x: List[((Int,Int),Matrix)] = List()
                    for((i,c) <- a) {
                      for((k,d) <- b) {
                        x :+= ((i,k), multBlocks(c,transposeBlock(d)))
                      }
                    }
                    x
                  }}
                  .reduceByKey{case (a,b) => sumBlocks(a,b,(g,h)=>g+h)}
        println(C.count())
        //C.collect.map(i => i._2.toArray.map(println))
      } catch { case x: Throwable => println(x); return -1.0 }
      (System.currentTimeMillis()-t)/1000.0
    }

    // hand written matrix multiplication of 3  matrices
    def testMultipyHandWritten2 (): Double = {
      val t = System.currentTimeMillis()
      try {
        val C = Am.map{case ((i,j),a) => (j,(i,a))}
                  .cogroup(Bm.map{case ((j,k),b) => (j, (k,b))})
                  .flatMap{case (j,(a,b)) => {
                    var x: List[(Int,(Iterable[(Int,Matrix)],Matrix))] = List()
                    for((k,d) <- b) {
                      x :+= (k, (a,d))
                    }
                    x
                  }}
                  .cogroup(Am.map{case ((j,k),a) => (j, (k,a))})
                  .flatMap{case (j,(a,b)) => {
                    var x: List[((Int,Int),Matrix)] = List()
                    for((c,d) <- a) {
                      for((i,f) <- c) {
                        for((k,l) <- b) {
                          x :+= ((i,k), multBlocks(multBlocks(transposeBlock(f),transposeBlock(d)),transposeBlock(l)))
                        }
                      }
                    }
                    x
                  }}
                  /*.reduceByKey{case (a,b) => sumBlocks(a,b,(g,h)=>g+h)}*/
        println(C.count())
        //C.collect.map(i => i._2.toArray.map(println))
      } catch { case x: Throwable => println(x); return -1.0 }
      (System.currentTimeMillis()-t)/1000.0
    }

    // hand written matrix multiplication of 3  matrices
    def testMultipyHandWritten3 (): Double = {
      val t = System.currentTimeMillis()
      try {
        val C = Am.map{case ((i,j),a) => (j,(i,a))}
                  .cogroup(Bm.map{case ((j,k),b) => (j, (k,b))})
                  .flatMap{case (j,(a,b)) => {
                    var x: List[(Int,(Iterable[(Int,Matrix)],Matrix))] = List()
                    for((k,d) <- b) {
                      x :+= (k, (a,d))
                    }
                    x
                  }}
                  .cogroup(Am.map{case ((j,k),a) => (j, (k,a))})
                  .flatMap{case (j,(a,b)) => {
                    var x: List[(Int,Matrix)] = List()
                    for((c,d) <- a) {
                      for((i,f) <- c) {
                        x :+= (i,multBlocks(transposeBlock(f),transposeBlock(d)))
                      }
                    }
                    var y: List[((Int,Int),Matrix)] = List()
                    for((i,d) <- x) {
                      for((k,l) <- b) {
                        y :+= ((i,k), multBlocks(d,transposeBlock(l)))
                      }
                    }
                    y
                  }}
                  .reduceByKey{case (a,b) => sumBlocks(a,b,(g,h)=>g+h)}
        println(C.count())
        //C.collect.map(i => i._2.toArray.map(println))
      } catch { case x: Throwable => println(x); return -1.0 }
      (System.currentTimeMillis()-t)/1000.0
    }

    def getEmptyMatrix(mat: Matrix): Matrix = {
      val cols = mat.numCols
      val rows = mat.numRows
      var arr = Array.ofDim[Double](cols*rows)
      for(i <- (0 to cols-1).par) {
          for(j <- 0 to rows-1) {
              arr(i*rows+j) = 0.0
          }
      }
      new DenseMatrix(cols,rows,arr)
    }
    // hand written matrix multiplication of 3  matrices
    def testMultipyHandWritten4 (): Double = {
      val t = System.currentTimeMillis()
      try {
        val C = Am.map{case ((i,j),a) => (j,(i,a))}
                  .cogroup(Bm.map{case ((j,k),b) => (j, (k,b))})
                  .flatMap{case (j,(a,b)) => {
                    var x: List[((Int,Int),Matrix)] = List()
                    for((i,c) <- a) {
                      for((k,d) <- b) {
                        x :+= ((i,k), multBlocks(transposeBlock(c),transposeBlock(d)))
                      }
                    }
                    x
                  }}
                  .mapPartitions(it => it.foldLeft(new HashMap[(Int,Int),Matrix])(
                    (arrMap,key) => arrMap += (key._1 -> sumBlocks(arrMap.getOrElse(key._1,getEmptyMatrix(key._2)),key._2,(g,h)=>g+h))).toIterator)
                  .map{case ((i,j),d) => (j,(i,d))}
                  .cogroup(Am.map{case ((j,k),a) => (j, (k,a))})
                  .flatMap{case (j,(a,b)) => {
                    var x: List[((Int,Int),Matrix)] = List()
                    for((i,c) <- a) {
                      for((k,d) <- b) {
                        x :+= ((i,k), multBlocks(c,transposeBlock(d)))
                      }
                    }
                    x
                  }}
                  .reduceByKey{case (a,b) => sumBlocks(a,b,(g,h)=>g+h)}
        println(C.count())
        //C.collect.map(i => i._2.toArray.map(println))
      } catch { case x: Throwable => println(x); return -1.0 }
      (System.currentTimeMillis()-t)/1000.0
    }

    def testMultipyHandWritten5 (): Double = {
      val t = System.currentTimeMillis()
      try {
        val C = Am.map{case ((i,j),a) => (j,(i,(i,j)))}
                  .cogroup(Bm.map{case ((j,k),b) => (j, (k,(j,k)))})
                  .flatMap{case (j,(a,b)) => {
                    var x: List[((Int,Int),((Int,Int),(Int,Int)))] = List()
                    for((i,c) <- a) {
                      for((k,d) <- b) {
                        x :+= ((i,k), (c,d))
                      }
                    }
                    x
                  }}
                  .map{case ((i,j),d) => (j,(i,d))}
                  .cogroup(Am.map{case ((j,k),a) => (j, (k,a))})
                  .flatMap{case (j,(a,b)) => {
                    var x: List[((Int,Int),Matrix)] = List()
                    for((i,c) <- a) {
                      val a1 = Am.filter{case ((e,f),am) => e==c._1._1 && f==c._1._2}.map{ case (e,v) => v}.collect().head
                      val b1 = Bm.filter{case ((e,f),bm) => e==c._2._1 && f==c._2._2}.map{ case (e,v) => v}.collect().head
                      for((k,d) <- b) {
                        x :+= ((i,k), block_mult(block_mult(a1,b1),d))
                      }
                    }
                    x
                  }}
                  .reduceByKey{case (a,b) => sumBlocks(a,b,(g,h)=>g+h)}
        println(C.count())
        //C.collect.map(i => i._2.toArray.map(println))
      } catch { case x: Throwable => println(x); return -1.0 }
      (System.currentTimeMillis()-t)/1000.0
    }

    val ADF = Am.map{ case ((i,j),v) => (i,j,v) }.toDF("I", "J", "TILE")
    val BDF = Bm.map{ case ((i,j),v) => (i,j,v) }.toDF("I", "J", "TILE")

    ADF.createOrReplaceTempView("A")
    BDF.createOrReplaceTempView("B")

    def force ( df: DataFrame ) {
      df.write.mode("overwrite").format("noop").save()
    }

    def mult_tiles ( a: DenseMatrix, b: DenseMatrix ): DenseMatrix
      = a.multiply(b.asInstanceOf[DenseMatrix])
    spark.udf.register("mult_tiles", udf(mult_tiles(_,_)))

    def tile_sum ( a: Seq[DenseMatrix] ): DenseMatrix = {
      val nd = a.head.numRows
      val md = a.head.numCols
      val s = Array.ofDim[Double](nd*md)
      for { x <- a
            i <- (0 until nd).par
            j <- 0 until md }
        s(i*md+j) += x(i,j)
      new DenseMatrix(nd,md,s)
    }
    spark.udf.register("tile_sum", udf(tile_sum(_)))

    def sum_tiles ( a: DenseMatrix, b: DenseMatrix ): DenseMatrix = {
      val nd = a.numRows
      val md = a.numCols
      val s = Array.ofDim[Double](nd*md)
      for { i <- (0 until nd).par
            j <- 0 until md }
        s(i*md+j) = a(i,j)+b(i,j)
      new DenseMatrix(nd,md,s)
    }
    spark.udf.register("sum_tiles", udf(sum_tiles(_,_)))

    /*def testMultiplySQL (): Double = {
      var t = System.currentTimeMillis()
      try {
        for(itr <- 0 to 9) {
          val C = spark.sql(""" SELECT A.I as I, B.J as J, tile_sum(collect_list(mult_tiles(A.TILE,B.TILE))) AS TILE
                                FROM A JOIN B ON A.J = B.I
                                GROUP BY A.I, B.J
                            """)
          ADF = C
          ADF.createOrReplaceTempView("A")
        }
        //force(C)
        val result = new BlockMatrix(ADF.rdd.map{ case Row( i:Int, j: Int, m: DenseMatrix ) => ((i,j),m) },N,N)
        println(result.blocks.count())
        //result.blocks.collect.map(i => i._2.toArray.map(println))
      } catch { case x: Throwable => println(x); return -1.0 }
      (System.currentTimeMillis()-t)/1000.0
    }*/

    /*def testMultiplySQL (): Double = {
      var t = System.currentTimeMillis()
      try {
          val C = spark.sql(""" SELECT A.I as I, A.J as J, sum_tiles(A.TILE,B.TILE) AS TILE
                                FROM A, B Where A.I = B.I AND A.J = B.J
                            """)
        //force(C)
        val result = new BlockMatrix(C.rdd.map{ case Row( i:Int, j: Int, m: DenseMatrix ) => ((i,j),m) },N,N)
        println(result.blocks.count())
        //result.blocks.collect.map(i => i._2.toArray.map(println))
      } catch { case x: Throwable => println(x); return -1.0 }
      (System.currentTimeMillis()-t)/1000.0
    }*/

    def testMultiply_ (): Double = {
      var t = System.currentTimeMillis()
      try {
        val A1 = randomTileSparse(N,N)
        val A2 = randomTileSparse(N,N)
        val A3 = A1.toDense
        val A4 = A2.toDense
        val result = A3.multiply(A4)
      } catch { case x: Throwable => println(x); return -1.0 }
      (System.currentTimeMillis()-t)/1000.0
    }

    def testMultiply_1 (): Double = {
      var t = System.currentTimeMillis()
      try {
        val A1 = randomTile(N,N)
        val A2 = randomTile(N,N)
        val result = A1.multiply(A2)
      } catch { case x: Throwable => println(x); return -1.0 }
      (System.currentTimeMillis()-t)/1000.0
    }

    def test ( name: String, f: => Double ) {
      val cores = Runtime.getRuntime().availableProcessors()
      var i = 0
      var j = 0
      var s = 0.0
      while ( i < repeats && j < 10 ) {
        val t = f
        j += 1
        if (t > 0.0) {   // if f didn't crash
          if(i > 0) s += t
          i += 1
          println("Try: "+i+"/"+j+" time: "+t)
        }
      }
      if (i > 0) s = s/(i-1)
      print("*** %s cores=%d n=%d m=%d N=%d ".format(name,cores,n,m,N))
      println("tries=%d %.3f secs".format(i,s))
    }

    //test("DIABLO Multiply ",testMultiply)
    //test("DIABLO Multiply Diablo-Join ",testMultipyDiabloJoin)
    test("MLlib Multiply ",testMultipyMLlib)
    /*else if(func == 2) test("DIABLO Multiply ",testMultipyDiablo)
    else if(func == 3) test("Diablo 1 Multiply ",testMultipyDiablo1)*/
    test("Handwritten Multiply ",testMultipyHandWritten)
    test("Handwritten Multiply 2",testMultipyHandWritten_1)
    //test("Handwritten 1 Multiply ",testMultipyHandWritten1)
    /*else if(func == 6) test("Handwritten 2 Multiply ",testMultipyHandWritten2)
    else if(func == 7) test("Handwritten 3 Multiply ",testMultipyHandWritten3)
    else if(func == 8) test("Handwritten 4 Multiply ",testMultipyHandWritten4)*/
    test("Handwritten 5 Multiply ",testMultipyHandWritten5)
    //test("SQL Multiply ",testMultiplySQL)
    //test("Multiply ",testMultiply_)
    //test("Multiply 1",testMultiply_1)

    spark_context.stop()
  }
}
