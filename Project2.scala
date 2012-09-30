import actors.Actor
import scala.actors.Actor._
import scala.util.Random

/**
 * Created with IntelliJ IDEA.
 * User: DIV
 * Date: 9/30/12
 * Time: 1:05 AM
 * To change this template use File | Settings | File Templates.
 */
case object Rumour
case object TransmitData

object Project2 {

  var num_nodes: Int = 0
  var topology, algorithm = ""

  def main(args: Array[String]) {
    collect_input
    start_process
    }

  def collect_input(){
    println("Enter the number of nodes :")
    num_nodes = readInt()
    println("Enter the topology :")
    topology = readLine()
    println("Enter the Algorithm :")
    algorithm = readLine();
  }

  def start_process(){
    topology match{
      case "Full" => {
        var full_topology = new FullTopology(num_nodes)
        full_topology.initiate_nodes()
        full_topology.g_nodes(0) ! Rumour
      }
    }
  }
}

abstract class Topology(num_nodes: Int) {
  var deactive_nodes:Array[Int] = Array()

  def increment_status(){
    println("Incrementing status and deactivating")
    deactive_nodes:+=1

  }
  def get_random_neighbour(): Actor
}

class FullTopology(num_nodes: Int) extends Topology(num_nodes) {
  var g_nodes:Array[GNodes] = Array()
  val rand = new Random()

  def initiate_nodes(){
    for( i <- 0 to num_nodes-1){
      g_nodes:+= new GNodes(i, this, num_nodes)
      g_nodes(i).start()
    }
  }

  override def get_random_neighbour(): Actor = {
    g_nodes(rand.nextInt(g_nodes.size))
  }
}

class GNodes(index : Int,topology: Topology, gnode_size: Int) extends Actor {
  var status: Boolean = true
  var count = 0
  val max_count = 10
  var started:Boolean = false

  def act(){
    loop {
      react{
        case Rumour => {
          count = count + 1
          println("Count of "+ this.index + " = "+ count)
          //for (i <- 0 to topology.deactive_nodes.size-1)
            //print(topology.deactive_nodes(i) + ",")
          if (topology.deactive_nodes.size == gnode_size){
            println("All nodes have received 10 each !")
            System.exit(0)
          }
          else if (count < max_count)
            self ! TransmitData
          else{
            if(status)
              topology.increment_status
            status = false
          }
        }

        case TransmitData => {
          if (count < max_count){
            topology.get_random_neighbour ! Rumour
            Thread.sleep(100)
            self ! TransmitData
          }

        }

      }
    }
  }

}




