import actors.Actor
import scala.actors.Actor._
import scala.util.Random
import java.lang.Math
import scala.Array._


/**
 * Created with IntelliJ IDEA.
 * User: DIV
 * Date: 9/30/12
 * Time: 1:05 AM
 * To change this template use File | Settings | File Templates.
 */
case object Rumour
case object TransmitData
case class PushSum(s: Int, w: Int)
case object TransmitPSData

object ConvergenceTime{
  var start_time:Long = 0
  var end_time:Long = 0
}

object Project2 {

  var num_nodes = 0
  var topology, algorithm = ""
  var failure:Boolean = false

  def main(args: Array[String]) {
    collect_input
    start_process
  }

  def collect_input() {
    println("Enter the number of nodes :")
    num_nodes = readInt()
    println("Enter the topology :")
    topology = readLine()
    println("Enter the Algorithm :")
    algorithm = readLine();
    println("Do you want to check failure ? (Y/N)")
    failure = readBoolean()
  }

  def start_process() {
    var is_pnode = false
    if (algorithm == "PushSum" || algorithm == "pushsum"|| algorithm == "Pushsum")
      is_pnode = true
    topology match {
      case "FULL" | "full" | "Full" => {
        var full_topology = new FullTopology(num_nodes)
        full_topology.initiate_nodes(is_pnode,failure)
        ConvergenceTime.start_time = System.currentTimeMillis()
        if(is_pnode)
          full_topology.g_nodes(0) ! PushSum(0,0)
        else
          full_topology.g_nodes(0) ! Rumour
      }

      case "line" | "LINE" | "Line" => {
        var line_topology = new LineTopology(num_nodes)
        line_topology.initiate_nodes(is_pnode,failure)
        ConvergenceTime.start_time = System.currentTimeMillis()
        if(is_pnode)
          line_topology.g_nodes(0) ! PushSum(0,0)
        else
          line_topology.g_nodes(0) ! Rumour
      }

      case "2D" | "2d" => {
        var twod_topology = new TwoDTopology(num_nodes)
        twod_topology.initiate_nodes(is_pnode,failure)
        ConvergenceTime.start_time = System.currentTimeMillis()
        if (is_pnode)
          twod_topology.g_nodes(0)(0) ! PushSum(0,0)
        else
          twod_topology.g_nodes(0)(0) ! Rumour
      }

      case "2DImp" | "2dimp" | "2Dimp" => {
        var twod_topology = new TwoDImpTopology(num_nodes)
        twod_topology.initiate_nodes(is_pnode,failure)
        ConvergenceTime.start_time = System.currentTimeMillis()
        if (is_pnode)
          twod_topology.g_nodes(0)(0) ! PushSum(0,0)
        else
          twod_topology.g_nodes(0)(0) ! Rumour
      }
    }
  }
}

abstract class Topology(num_nodes: Int) {
  var deactive_nodes: Array[Int] = Array()

  def increment_status(size:Int) {
    deactive_nodes :+= 1
    println("Incrementing status and deactivating : " + deactive_nodes)
    if (deactive_nodes.size == size) {
      println("All nodes have converged !")
      exit_process()
    }
  }

  def exit_process(){
    ConvergenceTime.end_time = System.currentTimeMillis()
    var time_diff = ConvergenceTime.end_time - ConvergenceTime.start_time
    println("Convergence Time = " + time_diff)
    System.exit(0)
  }

  def should_exit(sw_array:Array[BigDecimal]):Boolean = {
    if(sw_array(sw_array.size - 3)-sw_array(sw_array.size - 1) <= Math.pow(10,-10))
      return true
    else
      return false
  }

  def change_status{

  }

  def get_random_neighbour(): Actor

  def get_random_neighbour(index: Int): Actor

  def get_neighbours(index: Tuple2[Int, Int]): Array[Actor]

  def get_imperfect_neighbours(index: Tuple2[Int, Int]): Array[Actor]
}

class FullTopology(num_nodes: Int) extends Topology(num_nodes) {
  var g_nodes: Array[GNodes] = Array()
  val rand = new Random()

  def initiate_nodes(is_pnode: Boolean,failure: Boolean) {
    for (i <- 0 to num_nodes - 1) {
      g_nodes :+= new GNodes(i, this, num_nodes)
      if (is_pnode){
        g_nodes(i).is_pnode = is_pnode
        g_nodes(i).s = i
      }
      if (failure){
        g_nodes(rand.nextInt(g_nodes.size)).status = false
      }
      g_nodes(i).start()
    }
  }

  override def get_random_neighbour(): Actor = {
    var random_number:Int = 0
    random_number = rand.nextInt(g_nodes.size)
    g_nodes(random_number)
  }

  override def get_random_neighbour(index: Int): Actor = {
    var random_number:Int = 0
    do{
      random_number = rand.nextInt(g_nodes.size)
    }while(random_number == index)
    g_nodes(random_number)
  }

  override def get_neighbours(index: Tuple2[Int, Int]):Array[Actor] = {
    Array(g_nodes(0))
  }

  override def get_imperfect_neighbours(index: Tuple2[Int, Int]): Array[Actor] = {
    Array(g_nodes(0))
  }
}

class LineTopology(num_nodes: Int) extends Topology(num_nodes) {
  var g_nodes: Array[GNodes] = Array()
  var rand = new Random()
  def initiate_nodes(is_pnode: Boolean,failure: Boolean) {
    for (i <- 0 to num_nodes - 1) {
      g_nodes :+= new GNodes(i, this, num_nodes)
      if (is_pnode){
        g_nodes(i).is_pnode = is_pnode
        g_nodes(i).s = i
      }
      if (failure){
        g_nodes(rand.nextInt(g_nodes.size)).status = false
      }
      g_nodes(i).start()
    }
  }

  override def get_random_neighbour(): Actor = {
    g_nodes(0)
  }

  override def get_neighbours(index: Tuple2[Int, Int]):Array[Actor] = {
    Array(g_nodes(0))
  }

  override def get_imperfect_neighbours(index: Tuple2[Int, Int]): Array[Actor] = {
    Array(g_nodes(0))
  }

  override def get_random_neighbour(index: Int): Actor = {
    index match {
      case 0 => return g_nodes(1)

      case other:Int => {
        if (other == g_nodes.size - 1)
           return g_nodes(other - 1)
        (new Random().nextInt(10) % 2) match{
          case 0 => return g_nodes(other - 1)
          case 1 => return g_nodes(other + 1)
        }
      }
    }
  }
}

class TwoDTopology(num_nodes: Int) extends Topology(num_nodes){
  var use_val:Int = find_dimension(num_nodes)
  var g_nodes = ofDim[TwoDGNodes](use_val,use_val)
  var neighbour_indices: Array[Tuple2[Int, Int]] = Array()
  var rand = new Random()

  def initiate_nodes(is_pnode: Boolean,failure: Boolean) {
    var index:Tuple2[Int,Int] = (0,0)
    var node_count = 0
    for (i <- 0 to use_val - 1) {
      for (j<- 0 to use_val - 1){
        index = (i,j)
        g_nodes(i)(j) = new TwoDGNodes(index, this, (use_val*use_val))
        if (is_pnode){
          g_nodes(i)(j).is_pnode = is_pnode
          g_nodes(i)(j).s = node_count
          node_count = node_count + 1
        }
        g_nodes(i)(j).start()
      }
    }
    if (failure){
      var x = rand.nextInt(use_val)
      var y = rand.nextInt(use_val)
      println("X and Y values =" + x + ", " + y)
      (g_nodes(x)(y)).status = false
    }
  }

  def find_dimension(num_nodes:Int):Int = {
    var node = (Math.ceil(Math.sqrt(num_nodes)))
    println("Dimension is : "+ node)
    return (node).toInt
  }

  override def get_neighbours(index: Tuple2[Int, Int]):Array[Actor] = {
    var neighbours:Array[Actor] = Array()
    var x = index._1
    var y = index._2
    for (i <- 0 to use_val - 1) {
      for (j<- 0 to use_val - 1){
        if (x == 0 || y == 0){
          if (x!= use_val -1){
            neighbours:+= g_nodes(x+1)(y)
            neighbour_indices:+=((x+1),(y))
          }
          if (y!=use_val -1){
            neighbours:+= g_nodes(x)(y+1)
            neighbour_indices:+=((x),(y+1))
          }
          if (y!=0){
            neighbours:+= g_nodes(x)(y-1)
            neighbour_indices:+=((x),(y-1))
          }
          if (x!=0){
            neighbours:+= g_nodes(x-1)(y)
            neighbour_indices:+=((x-1),(y))
          }
        }

        else if (x == use_val - 1){
          neighbours:+= g_nodes(x-1)(y)
          neighbour_indices:+=((x-1),(y))
          if (y!= use_val -1){
            neighbours:+= g_nodes(x)(y+1)
            neighbour_indices:+=((x),(y+1))
          }
          if (y!=0){
            neighbours:+= g_nodes(x)(y-1)
            neighbour_indices:+=((x),(y-1))
          }
        }

        else if (y == use_val - 1){
          neighbours:+= g_nodes(x)(y-1)
          neighbour_indices:+=((x),(y-1))
          if (x!=use_val - 1){
            neighbours:+= g_nodes(x+1)(y)
            neighbour_indices:+=((x+1),(y))
          }
          if (x!=0){
            neighbours:+= g_nodes(x-1)(y)
            neighbour_indices:+=((x-1),(y))
          }
        }

        else{
          neighbours:+= g_nodes(x)(y-1)
          neighbour_indices:+=((x),(y-1))
          neighbours:+= g_nodes(x)(y+1)
          neighbour_indices:+=((x),(y+1))
          neighbours:+= g_nodes(x-1)(y)
          neighbour_indices:+=((x-1),(y))
          neighbours:+= g_nodes(x+1)(y)
          neighbour_indices:+=((x+1),(y))
        }

      }
    }

    return neighbours
  }

  override def get_imperfect_neighbours(index: Tuple2[Int, Int]): Array[Actor] = {
    var neighbours = get_neighbours(index)
    var possible_neighbours: Array[Actor] = Array()

    for (i <- 0 to use_val - 1) {
      for (j<- 0 to use_val - 1){
        var flag = false
        for (ind <- neighbour_indices){
          if (i == ind._1 && j == ind._2)
            flag = true
        }
        if(!flag)
          possible_neighbours:+= g_nodes(i)(j)
      }}

    neighbours:+= possible_neighbours(new Random().nextInt(1000) % possible_neighbours.size)

    return neighbours
  }

  override def get_random_neighbour(): Actor = {
    g_nodes(0)(0)
  }

  override def get_random_neighbour(index: Int): Actor = {
    g_nodes(0)(0)
  }
}

class TwoDImpTopology(num_nodes: Int) extends TwoDTopology(num_nodes){}

class GNodes(index: Int, topology: Topology, gnode_size: Int) extends Actor {
  var status: Boolean = true
  var count = 0
  val max_count = 5
  var s = 0
  var w = 1
  var is_pnode = false
  var last_received: Array[BigDecimal] = Array()
  var transmit_count = 0

  def act() {
    loop {
      react {
        case Rumour => {
          transmit_count = 0
          count = count + 1
          println("topology.deactive_nodes.size : "+ topology.deactive_nodes.size)
          if (count < max_count)
            self ! TransmitData
          else {
            if (status)
              topology.increment_status(gnode_size)
            status = false
          }
        }

        case TransmitData => {
          if (count < max_count) {
            if (topology.isInstanceOf[FullTopology])
              topology.get_random_neighbour(index) ! Rumour
            else if (topology.isInstanceOf[LineTopology])
              topology.get_random_neighbour(index) ! Rumour
            Thread.sleep(100)
            if (transmit_count >= (gnode_size+max_count)*2){
              println("Force Exit due to non convergence")
              println("Nodes converged = "+ topology.deactive_nodes.size)
              topology.exit_process()
            }

            transmit_count +=1
            self ! TransmitData
          }
        }

        case PushSum(rs: Int, rw: Int) => {
          transmit_count = 0
          s += rs
          w += rw
          if (w == 0)
            last_received :+= BigDecimal.apply(0)
          else
            last_received :+= BigDecimal.apply(s/w)

          if (status){
            if (last_received.size >= 3 && topology.should_exit(last_received)){
              topology.increment_status(gnode_size)
              status = false
            }
            else{
              self ! TransmitPSData
            }
          }
        }

        case TransmitPSData => {
          if (status){
            if (topology.isInstanceOf[FullTopology])
              topology.get_random_neighbour(index) ! PushSum(s/2, w/2)
            else if (topology.isInstanceOf[LineTopology])
              topology.get_random_neighbour(index) ! PushSum(s/2, w/2)
            if (transmit_count >= (gnode_size+max_count)*2){
              println("Force Exit due to non convergence")
              println("Nodes converged = "+ topology.deactive_nodes.size)
              topology.exit_process()
            }
            transmit_count += 1
            s = s/2
            w = w/2
            Thread.sleep(100)
            self ! TransmitPSData
          }
        }
      }
    }
  }
}


class TwoDGNodes(index: Tuple2[Int, Int], topology: Topology, gnode_size: Int) extends Actor {
  var status: Boolean = true
  var count = 0
  val max_count = 5
  var neighbours:Array[Actor] = Array()
  var s = 0
  var w = 1
  var is_pnode = false
  var last_received: Array[BigDecimal] = Array()
  var transmit_count = 0

  def act() {

    if (topology.isInstanceOf[TwoDTopology])
      neighbours = topology.get_neighbours(index)
    else if (topology.isInstanceOf[TwoDImpTopology])
      neighbours = topology.get_imperfect_neighbours(index)

    loop {
      react {
        case Rumour => {
          transmit_count = 0
          println("topology.deactive_nodes.size : "+ topology.deactive_nodes.size)
          count = count + 1
          if (count < max_count)
            self ! TransmitData
          else {
            if (status)
              topology.increment_status(gnode_size)
            status = false
          }
        }

        case TransmitData => {
          if (count < max_count) {
            get_random_neighbour() ! Rumour
            Thread.sleep(100)
            if (transmit_count >= (gnode_size+max_count)*2){
              println("Force Exit due to non convergence")
              println("Nodes converged = "+ topology.deactive_nodes.size)
              topology.exit_process()
            }

            transmit_count +=1
            self ! TransmitData
          }
        }

        case PushSum(rs: Int, rw: Int) => {
          transmit_count = 0
          s += rs
          w += rw
          if (w == 0)
            last_received :+= BigDecimal.apply(0)
          else
            last_received :+= BigDecimal.apply(s/w)

          if (status){
            if (last_received.size >= 3 && topology.should_exit(last_received)){
              topology.increment_status(gnode_size)
              status = false
            }
            else
              self ! TransmitPSData
          }
        }

        case TransmitPSData => {
          if(status){
            get_random_neighbour() ! PushSum(s/2, w/2)
            if (transmit_count >= (gnode_size+max_count)){
              println("Force Exit due to non convergence")
              println("Nodes converged = "+ topology.deactive_nodes.size)
              topology.exit_process()
            }
            transmit_count += 1
            s = s/2
            w = w/2
            Thread.sleep(100)
            self ! TransmitPSData
          }
        }
      }
    }
  }

  def get_random_neighbour():Actor = {
    neighbours(new Random().nextInt(10) % neighbours.size)
  }
}
