import edu.uta.diablo._
import org.apache.spark._
import org.apache.spark.rdd._
import org.apache.spark.mllib.linalg.distributed._
import org.apache.spark.mllib.linalg._
import org.apache.log4j._
import org.apache.hadoop.fs._
import scala.collection.Seq
import scala.util.Random
import Math._

object Join {
  /* The size of an object */
  def sizeof ( x: AnyRef ): Long = {
    import org.apache.spark.util.SizeEstimator.estimate
    estimate(x)
   }

  def main ( args: Array[String] ) {
    val repeats = args(0).toInt
    val n = args(1).toInt
    val m = if (args.length > 2) args(2).toInt else n
    val p = 10
    val sparsity = 0.01
    parami(block_dim_size,1000)  // size of each dimension in a block
    val N = 1000
    parami(number_of_partitions,10)
    //parami(broadcast_limit, 1000)

    val conf = new SparkConf().setAppName("diablojoin")
    spark_context = new SparkContext(conf)

    conf.set("spark.logConf","false")
    conf.set("spark.eventLog.enabled","false")
    LogManager.getRootLogger().setLevel(Level.WARN)

    def random () = {
      val rand = new Random()
      rand.nextDouble()*10
    }

    def randomTile ( nd: Int, md: Int, num: Double ): DenseMatrix = {
      val rand = new Random()
      val max = 10
      //new DenseMatrix(nd,md,Array.tabulate(nd*md){ i => rand.nextDouble()*max })
      new DenseMatrix(nd,md,Array.tabulate(nd*md){ i => num })
    }

    def randomMatrix ( rows: Int, cols: Int, rdim: Int, cdim: Int, num: Double ): RDD[((Int, Int),org.apache.spark.mllib.linalg.Matrix)] = {
      val l = Random.shuffle((0 until (rows+rdim-1)/rdim).toList)
      val r = Random.shuffle((0 until (cols+cdim-1)/cdim).toList)
      spark_context.parallelize(for { i <- l; j <- r } yield (i,j),number_of_partitions)
                   .map{ case (i,j) => ((i,j),randomTile(if ((i+1)*rdim > rows) rows%rdim else rdim,
                                                         if ((j+1)*cdim > cols) cols%cdim else cdim, num)) }
    }

    val Am = randomMatrix(n,m,N,N,2.0).cache()
    val Bm = randomMatrix(m,n,N,N,1.0).cache()
    val Cm = randomMatrix(n,p,N,N,1.0).cache()

    type tiled_matrix = ((Int,Int),EmptyTuple,RDD[((Int,Int),((Int,Int),EmptyTuple,Array[Double]))])
    val et = EmptyTuple()
    //  block tensors
    /*val AA = ((n,m),et,Am.map{ case ((i,j),a) => ((i,j),((a.numRows,a.numCols),et,a.transpose.toArray)) })
    val BB = ((m,n),et,Bm.map{ case ((i,j),b) => ((i,j),((b.numRows,b.numCols),et,b.transpose.toArray)) })
    val CC = ((n,p),et,Cm.map{ case ((i,j),b) => ((i,j),((b.numRows,b.numCols),et,b.transpose.toArray)) })*/
    val AA = q("tensor*(10,1000)[ ((i,j),random()) | i <- 0..(10-1), j <- 0..(1000-1)]")
    val BB = q("tensor*(1000,10)[ ((i,j),random()) | i <- 0..(1000-1), j <- 0..(10-1)]")
    val CC = q("tensor*(10,10)[ ((i,j),random()) | i <- 0..(10-1), j <- 0..(10-1)]")
    //val AA = q("tensor*(n)[ (i,2.0) | i <- 0..(n-1)]")
    //val BB = q("tensor*(n)[ (i,1.0) | i <- 0..(n-1)]")
    AA._3.cache
    BB._3.cache
    CC._3.cache

    /*def testAddHandWritten(): Double = {
        var t = System.currentTimeMillis()
        try {
            val C = q("""
            tensor*(n,n)[ ((ii_1,ii_2),a+b) | ((ii_1,ii_2),a) <- AA, ((jj_1,jj_2),b) <- BB, ii_1 == jj_1, ii_2 == jj_2 ];
            """)
            C._3.count()
            C._3.collect.map(i => i._2._3.map(println))
        } catch { case x: Throwable => println(x); return -1.0 }
        (System.currentTimeMillis()-t)/1000.0
    }*/

    /*def testMultiplyHandWritten(): Double = {
        var t = System.currentTimeMillis()
        try {
            val C = q("""
            tensor*(n,n)[ ((i,j),a) | ((i,j),a) <- tensor*(n,n)[ ((ii_1,jj_2),+/v) | ((ii_1,ii_2),a) <- AA, ((jj_1,jj_2),b) <- BB, ii_2 == jj_1, let v = a*b, group by (ii_1,jj_2) ]];
            """)
            C._3.count()
            C._3.collect.map(i => i._2._3.map(println))
        } catch { case x: Throwable => println(x); return -1.0 }
        (System.currentTimeMillis()-t)/1000.0
    }*/

    // (A*B)*C
    def testMultiplyHandWritten2(): Double = {
      var t = System.currentTimeMillis()
      try {
          val C = q("""
          tensor*(10,10)[ ((ii_1,kk_2),+/v) | ((ii_1,jj_2),ab) <- tensor*(10,10)[((ii_1,jj_2),+/v1) | ((ii_1,ii_2),a) <- tensor*(10,100)[ ((i,j),random()) | i <- 0..(10-1), j <- 0..(1000-1)], 
          ((jj_1,jj_2),b) <- tensor*(100,1000)[ ((i,j),random()) | i <- 0..(1000-1), j <- 0..(10-1)], ii_2 == jj_1, let v1 = a*b, group by (ii_1,jj_2) ], 
            ((kk_1,kk_2),c) <- tensor*(1000,10)[ ((i,j),random()) | i <- 0..(10-1), j <- 0..(10-1)], jj_2 == kk_1, let v=ab*c, group by (ii_1,kk_2)];
          """)
          C._3.count()
          //C._3.collect.map(i => i._2._3.map(println))
      } catch { case x: Throwable => println(x); return -1.0 }
      (System.currentTimeMillis()-t)/1000.0
    }

    // A*(B*C)
    /*def testMultiplyHandWritten2(): Double = {
        var t = System.currentTimeMillis()
        try {
            val C = q("""
            tensor*(n,p)[ ((ii_1,kk_2),+/v) | ((ii_1,ii_2),a) <- AA, ((jj_1,kk_2),bc) <- tensor*(m,p)[((jj_1,kk_2),+/v1) | ((jj_1,jj_2),b) <- BB,((kk_1,kk_2),c) <- CC, jj_2 == kk_1, let v1 = b*c, group by (jj_1,kk_2) ], 
              ii_2 == jj_1, let v=a*bc, group by (ii_1,kk_2)];
            """)
            C._3.count()
            //C._3.collect.map(i => i._2._3.map(println))
        } catch { case x: Throwable => println(x); return -1.0 }
        (System.currentTimeMillis()-t)/1000.0
    }*/

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

    //test("Handwritten Add",testAddHandWritten)
    test("Handwritten Multiply",testMultiplyHandWritten2)

    spark_context.stop()
  }
}