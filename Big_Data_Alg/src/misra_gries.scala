import scala.collection.immutable.LazyList.cons
import scala.collection.immutable.ListMap


class Car (name: String, capacity : Int, year : Int){
    
    val carName : String = name
    val carCapacity : Int = capacity
    val carYear : Int = year
    
    def == (that: Car): Boolean = {
        if ((carName == that.carName) && (carCapacity == that.carCapacity) && (carYear == that.carYear)){
            return true
        }
        else{
            return false
        }
    }
}

class Book (title: String, author : String, publisher : String, year : Int){
    
    val bookTitle : String = title
    val bookAuthor : String = author
    val bookPublisher : String = publisher
    val bookYear : Int = year
    
    def == (that: Book): Boolean = {
        if ((bookTitle == that.bookTitle) && (bookAuthor == that.bookAuthor) && (bookPublisher == that.bookPublisher) && (bookYear == that.bookYear)){
            return true
        }
        else{
            return false
        }
    }
}

class Employee (name: String, surname : String, salary : Float, startingYear : Int){
    
    val empName : String = name
    val empSurname : String = surname
    val empSalary : Float = salary
    val empYear : Int = startingYear
    
    def == (that: Employee): Boolean = {
        if ((empName == that.empName) && (empSurname == that.empSurname) && (empSalary == that.empSalary) && (empYear == that.empYear)){
            return true
        }
        else{
            return false
        }
    }
}

object MisraGries{
    
    def misraGries[T](stream : LazyList[T], desFreq : Int) : ListMap[T, Int] = {
        val n = stream.length
        val k = n/(desFreq -1)

        var buffer = new ListMap[T, Int]
        
        stream.foreach {
            x => {
                if (buffer.exists({ case (key, _) => key == x })) {
                    buffer.foreach {
                        case (key, value) =>
                            if (key == x) {
                                buffer -= key
                                buffer += (key -> (value + 1))
                            }
                    }
                }
                else if (buffer.size < (k - 1)) {
                    buffer += (x -> 1)
                }
                else {
                    buffer.foreach {
                        case (key, value) =>
                            if (value == 1) {
                                buffer -= key
                            }
                            else {
                                buffer -= key
                                buffer += (key -> (value - 1))
                            }
                    }
                }
            }
        }
        return buffer
    }

    def extendedMisraGries[T](stream : LazyList[T], c : Class[_], desFreq : Int) : ListMap[T, Int] = {
        val new_stream = stream.filter(x => x.getClass == c)
        
        return misraGries(new_stream, desFreq)
    }


    def main(args:Array[String]): Unit = {
        val car1 = new Car("1", 1600, 1997)
        val car2 = new Car("2", 2000, 1997)
        val car3 = new Car("3", 1600, 1997)
        val car6 = new Car("6", 1600, 1997)
        val car7 = new Car("7", 1600, 1997)
        val car8 = new Car("8", 1600, 1997)
        val book1 = new Book("bk1", "Snow", "PUBG", 2012)

        val str = car1 #:: car2 #:: book1 #:: car3 #:: car2 #:: car6 #:: car7 #:: car8 #:: car2 #:: car2 #:: car1 #:: car3 #:: car3 #:: car1 #:: car1 #:: car3 #:: book1 #:: LazyList.empty
        var myList = extendedMisraGries(str, classOf[Car], 4)

        myList.foreach {
            case (key, value) =>
                val myCar = key.asInstanceOf[Car]
                println(myCar.carName + " : " + value)
        }
        println(myList)
    }
}