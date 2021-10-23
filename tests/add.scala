import edu.uta.diablo._
import org.apache.spark._
import org.apache.spark.rdd._
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.mllib.linalg.distributed._
import org.apache.spark.mllib.linalg._
import org.apache.log4j._
import org.apache.hadoop.fs._
import scala.collection.Seq
import scala.util.Random
import Math._

object Add {
  /* The size of any serializable object */
  def sizeof ( x: Serializable ): Int = {
    import java.io.{ByteArrayOutputStream,ObjectOutputStream}
    val bs = new ByteArrayOutputStream()
    val os = new ObjectOutputStream(bs)
    os.writeObject(x)
    os.flush()
    os.close()
    bs.toByteArray().length
  }

  def main ( args: Array[String] ) {
    val repeats = args(0).toInt   // how many times to repeat each experiment
    // each matrix has n*m elements
    val n = args(1).toInt
    val m = if (args.length > 2) args(2).toInt else n
    parami(block_dim_size,1000)  // size of each dimension in a block
    val N = 1000
    val validate_output = false
    parami(number_of_partitions,10)

    val conf = new SparkConf().setAppName("add")
    spark_context = new SparkContext(conf)
    conf.set("spark.logConf","false")
    conf.set("spark.eventLog.enabled","false")
    LogManager.getRootLogger().setLevel(Level.WARN)

    def randomTile ( nd: Int, md: Int ): DenseMatrix = {
      val max = 10
      val rand = new Random()
      new DenseMatrix(nd,md,Array.tabulate(nd*md){ i => rand.nextDouble()*max })
    }

    def randomMatrix ( rows: Int, cols: Int ): RDD[((Int, Int),org.apache.spark.mllib.linalg.Matrix)] = {
      val l = Random.shuffle((0 until (rows+N-1)/N).toList)
      val r = Random.shuffle((0 until (cols+N-1)/N).toList)
      spark_context.parallelize(for { i <- l; j <- r } yield (i,j))
                   .map{ case (i,j) => ((i,j),randomTile(if ((i+1)*N > rows) rows%N else N,
                                                         if ((j+1)*N > cols) cols%N else N)) }
    }

    val Am = randomMatrix(n,m).cache()
    val Bm = randomMatrix(n,m).cache()

    val A = new BlockMatrix(Am,N,N)
    val B = new BlockMatrix(Bm,N,N)

    type tiled_matrix = (Int,Int,RDD[((Int,Int),(Int,Int,Array[Double]))])

    // dense block tensors
    val AA = (n,m,Am.map{ case ((i,j),a) => ((i,j),(a.numRows,a.numCols,a.transpose.toArray)) })
    val BB = (n,m,Bm.map{ case ((i,j),a) => ((i,j),(a.numRows,a.numCols,a.transpose.toArray)) })
    var CC = AA

    val rand = new Random()
    def random () = rand.nextDouble()*10

    // sparse block tensors with 99% zeros
    val Az = q("tensor*(n)(m)[ ((i,j),random()) | i <- 0..(n-1), j <- 0..(m-1), random() > 9.9 ]")
    val Bz = q("tensor*(n)(m)[ ((i,j),random()) | i <- 0..(n-1), j <- 0..(m-1), random() > 9.9 ]")

    def validate ( M: tiled_matrix ) = {
      if (!validate_output)
        M._3.count()
      else {
        val C = A.add(B).toLocalMatrix()
        val CC = M._3.collect
        println("Validating ...")
        for { ((ii,jj),(nd,md,a)) <- CC
              i <- 0 until nd
              j <- 0 until md } {
           val ci = ii*N+nd
           if (Math.abs(a(i*md+j)-C(ii*N+i,jj*N+j)) > 0.01)
             println("Element (%d,%d)(%d,%d) is wrong: %.3f %.3f"
                     .format(ii,jj,i,j,a(i*md+j),C(ii*N+i,jj*N+j)))
        }
      }
    }

    // matrix addition of tiled matrices in MLlib.linalg
    def testAddMLlib: Double = {
      val t = System.currentTimeMillis()
      try {
        val C = A.add(B)
        val x = C.blocks.count
      } catch { case x: Throwable => println(x); return -1.0 }
      (System.currentTimeMillis()-t)/1000.0
    }

    // matrix addition of tiled matrices using Diablo array comprehensions
    def testAddDiabloDAC: Double = {
      val t = System.currentTimeMillis()
      try {
        val C = q("""
                  tensor*(n,m)[ ((i,j),a+b) | ((i,j),a) <- AA, ((ii,jj),b) <- BB, ii == i, jj == j ];
                  """)
        validate(C)
      } catch { case x: Throwable => println(x); return -1.0 }
      (System.currentTimeMillis()-t)/1000.0
    }

    // matrix addition of sparse tiled matrices using Diablo array comprehensions
    def testAddDiabloDACsparse (): Double = {
      val t = System.currentTimeMillis()
      try {
        val C = q("""
                  tensor*(n,m)[ ((i,j),a+b) | ((i,j),a) <= Az, ((ii,jj),b) <= Bz, ii == i, jj == j ];
                  """)
        C._3.count()
      } catch { case x: Throwable => println(x); return -1.0 }
      (System.currentTimeMillis()-t)/1000.0
    }

    // matrix addition of sparse-dense tiled matrices using Diablo array comprehensions
    def testAddDiabloDACsparseDense (): Double = {
      val t = System.currentTimeMillis()
      try {
        val C = q("""
                  tensor*(n,m)[ ((i,j),a+b) | ((i,j),a) <= Az, ((ii,jj),b) <- BB, ii == i, jj == j ];
                  """)
        C._3.count()
      } catch { case x: Throwable => println(x); return -1.0 }
      (System.currentTimeMillis()-t)/1000.0
    }

    // matrix addition of tiled matrices using loops
    def testAddDiabloDACloop: Double = {
      val t = System.currentTimeMillis()
      try {
        val C = q("""
                  for i = 0, n-1 do
                     for j = 0, m-1 do
                        CC[i,j] = AA[i,j]+BB[i,j];
                  CC;
                  """)
        validate(C)
      } catch { case x: Throwable => println(x); return -1.0 }
      (System.currentTimeMillis()-t)/1000.0
    }

    // matrix addition of tiled matrices using Diablo array comprehensions - no in-tile parallelism
    def testAddDiabloS: Double = {
      param(parallel,false)
      val t = System.currentTimeMillis()
      try {
        val C = q("""
                  tensor*(n,m)[ ((i,j),a+b) | ((i,j),a) <- AA, ((ii,jj),b) <- BB, ii == i, jj == j ]
                  """)
        validate(C)
      } catch { case x: Throwable => println(x); return -1.0 }
      param(parallel,true)
      (System.currentTimeMillis()-t)/1000.0
    }

    def testAddCode (): Double = {
      val t = System.currentTimeMillis()
      try {
        val C = (n,m,AA._3.join(BB._3)
                       .mapValues{ case ((nd,md,a),(_,_,b))
                                     => val c = Array.ofDim[Double](nd*md)
                                        for { i <- (0 until nd).par
                                              j <- 0 until md
                                            } c(i*md+j) = a(i*md+j)+b(i*md+j)
                                        (nd,md,c)
                                 })
        validate(C)
      } catch { case x: Throwable => println(x); return -1.0 }
      (System.currentTimeMillis()-t)/1000.0
    }

    val spark = SparkSession.builder().config(conf).getOrCreate()
    import spark.implicits._

    val ADF = Am.map{ case ((i,j),v) => (i,j,v) }.toDF("I", "J", "TILE")
    val BDF = Bm.map{ case ((i,j),v) => (i,j,v) }.toDF("I", "J", "TILE")

    ADF.createOrReplaceTempView("A")
    BDF.createOrReplaceTempView("B")

    def add_tiles ( a: DenseMatrix, b: DenseMatrix ): DenseMatrix = {
      val c = Array.ofDim[Double](a.numRows*a.numCols)
      for { i <- (0 until a.numRows).par
            j <- 0 until a.numCols
          } c(i*a.numCols+j) = a(i,j)+b(i,j)
      new DenseMatrix(a.numRows,a.numCols,c)
    }
    spark.udf.register("add_tiles", udf(add_tiles(_,_)))

    def testAddSQL (): Double = {
      var t = System.currentTimeMillis()
      try {
        val C = spark.sql(""" SELECT A.I, A.J, add_tiles(A.TILE,B.TILE) AS TILE
                              FROM A JOIN B ON A.I = B.I AND A.J = B.J
                          """)
        //println(C.queryExecution)
        val result = new BlockMatrix(C.rdd.map{ case Row( i:Int, j: Int, m: DenseMatrix ) => ((i,j),m) },N,N)
        result.blocks.count()
      } catch { case x: Throwable => println(x); return -1.0 }
      (System.currentTimeMillis()-t)/1000.0
    }

    println("@@@@ IJV matrix size: %.2f GB".format(sizeof(((1,1),0.0D)).toDouble*n*m/(1024.0*1024.0*1024.0)))
    val tile_size = sizeof(((1,1),randomTile(N,N))).toDouble
    println("@@@@ tile matrix size: %.2f GB".format(tile_size*(n/N)*(m/N)/(1024.0*1024.0*1024.0)))
    println("@@@@ sparse partition sizes: "+Az._3.map{ case (_,(_,_,(_,a,_))) => a.length }.collect.toList)

    def test ( name: String, f: => Double ) {
      val cores = Runtime.getRuntime().availableProcessors()
      var i = 0
      var j = 0
      var s = 0.0
      while ( i < repeats && j < 10 ) {
        val t = f
        j += 1
        if (t > 0.0) {   // if f didn't crash
          i += 1
          println("Try: "+i+"/"+j+" time: "+t)
          s += t
        }
      }
      if (i > 0) s = s/i
      print("*** %s cores=%d n=%d m=%d N=%d ".format(name,cores,n,m,N))
      println("tries=%d %.3f secs".format(i,s))
    }

    test("MLlib Add",testAddMLlib)
    test("DIABLO Add",testAddDiabloDAC)
    test("DIABLO Add sparse-sparse",testAddDiabloDACsparse)
    test("DIABLO Add sparse-dense",testAddDiabloDACsparseDense)
    test("DIABLO loop Add",testAddDiabloDACloop)
    test("Hand-written Add",testAddCode)
    test("SQL Add",testAddSQL)

    spark_context.stop()
  }
}
