import org.apache.spark.{SparkContext, SparkConf}
import org.apache.spark.SparkContext._

object SparkReducesideJoin{
  def main(args: Array[String]) {
    
    if (args.length < 2) { 
			System.err.println("Please provide the 2 input files full path as arguments"); 
			System.exit(0); 
		} 
    	
		case class Achats (nv: Int, nb: Int, dates: String, lieu: String, qte: Int)
    case class Vins (num: Int, cru: String, mill: String, degre: Float)
		
    //def achatMapper(r:String) : (Int, Achats) = {
    //  var ary = r.split(",");
    //return (ary(0).toInt, Achats(ary(0).toInt, ary(1).toInt, ary(2), ary(3), ary(4).toInt)
    //}
    
		//def vinMapper(c:String) : (Int, Vins) = {
    //  var ary = c.split(",");
    //  return (ary(0).toInt, Vins(ary(0).toInt, ary(1), ary(2), ary(3).toFloat)
    //}
    
    val conf = new SparkConf().setAppName("org.sparkexample.ReduceSideJoin"); 
		val sc = new SparkContext(conf); 
	
    try{
      val achats = sc.textFile(args(0)).map(_.split(",")).map(
       r => (r(0).toInt, Achats(r(0).toInt, r(1).toInt, r(2), r(3), r(4).toInt))
      )
    
      val vins = sc.textFile(args(1)).map(_.split(",")).map(
       c => (c(0).toInt, Vins(c(0).toInt, c(1), c(2), c(3).toFloat))
      )

      val result = achats.join(vins);
      result.saveAsTextFile(args(2)); //save result to local file or HDFS  
    } catch {
      case e: Exception => println("exception caught: " + e);
    }

  }
}