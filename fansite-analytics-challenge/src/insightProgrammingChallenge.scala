/**
 * NOTE: This is a first attempt at using Scala. Maybe not the most advisable, but it seemed like a good
 * opportunity/excuse, and it is likely to come up during the actual fellowship, so it's good to get started on it.
 * Being new to Scala and functional programming, this will probably be more imperative than it should be.
 * In hindsight, doing it in something familiar, like Perl or Java, might have made a lot more sense.'
 *
 * The collection implementations seem less memory efficient than I would have expected, which is a little troubling.
 * Too late to turn back now.
 *
 * To make sure the testing runs smoothly, I didn't want to depend on any third party libraries, just the standard stuff.
 * - Tim O'Connor 4/5/17
 */

import java.io._
import scala.util.matching.Regex
    
/**
 * This is going to be a publisher/subscriber model.
 *
 * There are two obvious parallelizable approaches to this. One is to build a large, centralized structure (ie. table)
 * and have workers churn through it doing their tasks. The other is to set up a publisher/subscriber model and pass
 * out the data as it becomes available.
 *
 * The first approach was rejected here because each feature benefits from unique structures that don't centralize well together.
 * Additionally there are a number of issues that arise in terms of managing the central table to free up memory as data
 * gets processed, assuming in production there may be a continuous stream, and multiple passes over the entire data set
 * (or large portions of it) may be needed to implement each feature. There may still be multiple passes in a publisher/subscriber
 * design, but they "feel more natural".
 *
 * As for the second approach, aside from appearing more efficient at first glance, 
 * it also offers an opportunity to demonstrate multiple data structure designs. In a production system,
 * particularly with a language such as Scala, it makes it easy to parallelize the design, as
 * more consumers can be added as needed.
 */
trait ConsumerTrait
{
  //Possible table columns, using a DB-style approach: host, request, bytes, request time, response code, table insert time, read count
  /**
   * This is the only real defining feature necessary to implement a consumer.
   * What the consumer does with this data is its own business, and the rest of the system
   * does not need to care.
   * Arguments:
   *  host - The IP/domain of the source of the request.
   *  resource - The requested request (ie. URI), not including the HTTP method and version or any other part of the request.
   *  bytes - The number of bytes in the response.
   *  time - The time in milliseconds using the standard epoch.
   *  response - The HTTP response code.
   *  line - The original, unparsed data.
   */
  def consume(host:String, resource:String, bytes:Int, time:Long, response:Int, line:String);
}

/**
 * This allows asynchronous flushing of output. This would not necessarily come up in a deployed/live system.
 * But, in this case, there's not a continuous data flow and there needs to be a signal to dump the output
 * to a text file. However, even in a scaled-up system, there may be a case for a similar feature.
 */
trait FlushableTrait
{
  def flush() : Unit;
}

/**
 * It is good practice to have a unique identifier for managing a collection (Otherwise, how do you know who to unsubscribe?).
 * This can be useful for extension, maintenance, testing, and scalability down the road.
 *
 * The consumers will be cycled through by worker threads, and the list will be sorted by ascending priority
 * (thus a lower number is a higher priority).
 * Feature 4 should be flagged as high priority, in the event that a production system has to prioritize
 * to meet timing constraints.
 */
abstract class Consumer(val uniqueID:String, var priority:Int, var outputFile:String) extends Ordered[Consumer] with ConsumerTrait with FlushableTrait
{
  //Cache this, because repeatedly opening and closing files is wasteful (especially important for feature4).
  var writer : BufferedWriter = null;

  override def toString = "Consumer " + this.getClass.getName + " -\n\tuniqueID: $uniqueID\n\tpriority: $priority";
  
  def compare(that: Consumer) = this.priority - that.priority;
  
  def getWriter() : BufferedWriter =
  {
    if (this.writer == null)
    {
      this.writer = new BufferedWriter(new FileWriter(new File(this.outputFile)));
    }
    return this.writer;
  }
}

/**
 * A simple wrapper for managing sets of Consumer instances, itself implemented as a Consumer.
 */
class ConsumerCollection(uniqueID:String, priority:Int, outputFile:String) extends TopXConsumer(uniqueID, priority, outputFile)
{
  private var consumers = scala.collection.mutable.HashMap.empty[String, Consumer];
  override def toString = "Consumer " + this.getClass.getName + " -\n\tuniqueID: $uniqueID\n\tpriority: $priority";
  
  def addConsumer(consumer:Consumer) : Unit =
  {
    this.consumers += consumer.uniqueID -> consumer;
    //Sort by priority. There's probably a better way to do this, but it isn't important now.
    this.consumers = scala.collection.mutable.HashMap(this.consumers.toSeq.sortWith(_._2 < _._2): _*);
  }
  
  def removeConsumer(uniqueID:String) : Unit =
  {
    this.consumers.remove(uniqueID);
  }
  
  def consume(host:String, request:String, bytes:Int, time:Long, response:Int, line:String)
  {
    for ((key, value) <- this.consumers)
    {
      value.consume(host, request, bytes, time, response, line);
    }
  }
  
  def flush()
  {
    for ((key, value) <- this.consumers)//This should use an iterator over the values.
    {
      value.flush();
    }
  }
}

/**
 * A wrapper so the Bandwidth class (below) can be interchanged with an Int wrapper (to share code for features 1 & 2).
 */
trait ToInt
{
  def toInt() : Int;
}

/**
 * Implementation of ToInt.
 */
class IntToIntWrapper(var value : Int) extends Ordered[ToInt] with ToInt
{
  def toInt() : Int = { return this.value; }
  def compare(that: ToInt) = this.value - that.toInt;
  def +(b : Int) : Int = { return this.value + b; }
  override def toString() : String = { return this.value.toString(); }
  def incrementBy(b : Int) = { this.value += b; }
}

/**
 * This is a helper class that collects the necessary information for computing bandwidth while
 * playing nice with maps and supporting sort operations.
 */
class Bandwidth(bytes:Int, timestamp:Long) extends Ordered[Bandwidth] with ToInt
{
  var totalBytes:Int = 0;
  var earliestTime:Long = Long.MaxValue;
  var latestTime:Long = 0;
  var numRequests:Int = 0;

  increment(bytes, timestamp);

  def increment(bytes:Int, timestamp:Long) : Unit =
  {
    this.numRequests += 1;
    this.totalBytes += bytes;
    this.earliestTime = math.min(timestamp, this.earliestTime);
    this.latestTime = math.max(timestamp, this.latestTime);
  }

  //Convert the object into a bandwidth value [bytes/s].
  def toBandwidth:Double =
  {
//println("earliest: " + this.earliestTime + "  latest: " + this.latestTime + "  bytes: " + this.totalBytes);
    if ((this.earliestTime == Long.MaxValue) || (this.latestTime == 0))
      return -1;
    else if (this.earliestTime == this.latestTime)
      return this.totalBytes;
    else
      return this.totalBytes / ((this.latestTime - this.earliestTime) / 1000);//Don't forget to convert from milliseconds.
  }

  def toInt:Int = { return this.toBandwidth.toInt; }
  override def toString:String = { return this.toBandwidth.toString; }
  override def equals(o:Any): Boolean = o == this.toBandwidth;
  def compare(that: Bandwidth) = this.toBandwidth.toInt - that.toBandwidth.toInt;
}

//Provide methods for finding the top X table rows based on numeric data in the second column of a 2D Map (with no guaranteed order).
abstract class TopXConsumer(uniqueID:String, priority:Int, outputFile:String) extends Consumer(uniqueID, priority, outputFile)
{
  /**
   * Sorting the map with built-in methods is the "easiest" way to do this. But, that's computationally and
   * memory intensive. So, we're going to do our own little search, reducing the data as we go.
   * This should run in something closer to constant time (plus some fiddling with a much smaller map)
   * and consume negligible additional memory.
   *
   * The goal here is to limit the number of passes over the potentially very large map to just 1. All other
   * optimizations are secondary.
   */
  def getTopX(table:scala.collection.mutable.Map[String, ToInt], x:Int) : Seq[(String, Int)] =
  {
    //We will build a smaller map of just the top X counts, which may result in more than X elements because there can be multiple keys per count.
    var topX = scala.collection.mutable.HashMap.empty[String, Int];
    var minVal = 0;
    for ((key, value) <- table)
    {
      var intValue = value.toInt;
      //If we haven't filled out the mini-map this value gets added automatically.
      //Otherwise, we insert and prune once in a while.
      if ((topX.size < x) || (intValue >= minVal))
      {
        topX += key -> intValue;
        //minVal = math.min(topX.values);
        minVal = topX.minBy(_._2)._2;
        //Prune - This is not the most efficient implementation.
        //        But the data set is relatively small, and this is a clean/maintainable way of going about it.
        if (topX.size >= 2*x)
        {
          //Re-sort by vals, grab the highest vals, then alphabetically sort back into a smaller map.
          //We are keeping this map larger than strictly necessary to allow extra rows for possible alphabetic
          //sorting when there are value collisions. However, there may be a better approach for this...
          topX = scala.collection.mutable.HashMap(topX.toSeq.sortWith(_._2 > _._2).take(math.ceil(1.5*x).toInt): _*);
        }
      }
    }

    //Re-alphabetize.
    topX = scala.collection.mutable.HashMap(topX.toSeq.sortWith(_._1 < _._1): _*);
    return topX.toSeq.sortWith(_._2 > _._2).take(x);//Do a final sort
  }

  def flush(table:scala.collection.mutable.Map[String, ToInt])
  {
    val writer : BufferedWriter = this.getWriter;
    for ((k, v) <- this.getTopX(table, 10))
    {
      writer.write(k + "," + v);
      writer.newLine();
    }
    writer.close();
  }
}

/**
 * I am not thrilled with the names of these implementations, but it avoids ambiguity
 * in regards to the definition of the challenge.
 *
 * It seems wasteful to build an entire table to solve this particular problem, but it isn't possible to know
 * the answer without scanning all the data. The data can't be reduced until the full set has been loaded.
 * It is essentially a running tally and can't bypassed.
 */
class feature1(uniqueID:String, priority:Int, outputFile:String) extends TopXConsumer(uniqueID, priority, outputFile)
{
  //We need to keep a number (access count or bytes [bandwidth]) associated with each unique source. So, hashtable.
  //There needs to be a number paired to each string. Not really a better structure included in the language (if one exists at all).
  //There may be other hashtables in other features, which can result in per-host duplication of rows, but the data is different
  //and the replication seems better than the overhead of sharing hashtables across consumers. However, that is
  //a potential target for optimization.
  val table = scala.collection.mutable.HashMap.empty[String, ToInt];

  def consume(host:String, resource:String, bytes:Int, time:Long, response:Int, line:String)
  {
    //NOTE: Here we will not do any DNS look ups to identify IP/domain overlaps.
    val opt : Option[ToInt] = this.table.get(host);
    if (opt == None)
      this.table += host -> new IntToIntWrapper(1);
    else
      opt.get.asInstanceOf[IntToIntWrapper].incrementBy(1);//asInstanceOf is frowned on in Scala programming.
  }
  
  def flush()
  {
    this.flush(this.table);
  }
}

/**
 * There is a strong temptation to merge this with feature1, because the task is very similar.
 * But it is important to remember that they use different keys, and going down the road of a multi-keyed map doesn't seem wise.
 */
class feature2(uniqueID:String, priority:Int, outputFile:String) extends TopXConsumer(uniqueID, priority, outputFile)
{
  //We need to keep a number (access count or bytes [bandwidth]) associated with each unique source. So, hashtable.
  //There needs to be a number paired to each string. Not really a better structure included in the language (if one exists at all).
  //There may be other hashtables in other features, which can result in per-host duplication of rows, but the data is different
  //and the replication seems better than the overhead of sharing hashtables across consumers. However, that is
  //a potential target for optimization.
  val table = scala.collection.mutable.HashMap.empty[String, ToInt];

  def consume(host:String, resource:String, bytes:Int, time:Long, response:Int, line:String)
  {
    //Wrap the details into a Bandwidth object which will hide the messiness of multiple variables and calculations.
    val opt : Option[ToInt] = this.table.get(resource);
    if (opt == None)
      this.table += resource -> new Bandwidth(bytes, time);
    else
      opt.get.asInstanceOf[Bandwidth].increment(bytes, time);//asInstanceOf is frowned on in Scala programming.
  }
  
  def flush()
  {
    this.flush(this.table);
  }
}

/**
 * This is going to work on the assumption that data is passed in chronologically, so a List can be used
 * to store the data. This implementation follows the example supplied and starts the time windows on events.
 */
class feature3(uniqueID:String, priority:Int, outputFile:String) extends TopXConsumer(uniqueID, priority, outputFile)
{
  val hours = new scala.collection.mutable.MutableList[IntToIntWrapper];
  val hourStrings = new scala.collection.mutable.MutableList[String];
  var t0 : Long = 0;
  //This didn't need to be so complex, but since I worked it out, I'm leaving it in.
  var timestampPattern = new Regex(".+-\\s+\\[(\\d+{2}/\\D{3}/\\d{4}:\\d{2}:\\d{2}:\\d{2}\\s+-?\\d{3,5})\\]\\s+.+");

  def consume(host:String, resource:String, bytes:Int, time:Long, response:Int, line:String)
  {
    //Only create new list entries when the time window has elapsed.
    if (time - t0 > 60*60*1000)
    {
      line match
      {
        case this.timestampPattern(timestampStr) =>
          //Start a new hour window.
          hourStrings += timestampStr;
          hours += new IntToIntWrapper(1);
          t0 = time;
        case _ =>
          println("Error - Failed to parse timestamp from :: " + line);
      }
    }
    else
    {
      //Increment an existing value.
      this.hours.last.incrementBy(1);
    }
  }

  //We haven't been using a table, because sequence appends are cheaper than table insertions.
  //But, for the output, we'll call back to our table-based implementation.
  def flush()
  {
    import scala.collection.breakOut
    val table : scala.collection.mutable.Map[String, ToInt] = (this.hourStrings zip this.hours)(breakOut);
    this.flush(table);
  }
}

/**
 * This will work very similarly to feature1 and feature 2, but we will encode the date as the key, rounded down to the nearest hour.
 * This seems like the spirit of the specification for feature 3, but in the example shown the window boundaries are not at the
 * beginning of wall clock hours.
 */
class feature3A(uniqueID:String, priority:Int, outputFile:String) extends TopXConsumer(uniqueID, priority, outputFile)
{
  //One concern about this is that in the case of collisions in the number of accesses,
  //it will fall back to alphabetical sorting of the timestamp, while chronological sorting may be expected.
  //Since it isn't specified in the challenge, and the choice is somewhat arbitrary, this is fine for now.
  val table = scala.collection.mutable.HashMap.empty[String, ToInt];

  //Grab the timestamp string.
  var timestampPattern = new Regex(".+-\\s+\\[(\\d+{2}/\\D{3}/\\d{4}:\\d{2}:\\d{2}:\\d{2}\\s+-?\\d{3,5})\\]\\s+.+");
  //Replace the minutes and seconds to flatten the hours down to the first moment.
  var replacementPattern = new Regex("(?<=\\d{4}:\\d{2}:)\\d{2}:\\d{2}(?=\\s+.+)");

  def consume(host:String, resource:String, bytes:Int, time:Long, response:Int, line:String)
  {
    line match
    {
      case this.timestampPattern(timestampStr) =>
        //Replace the minutes and seconds
        val hourCode = this.replacementPattern.replaceFirstIn(timestampStr, "00:00");
        val opt : Option[ToInt] = this.table.get(hourCode);
        if (opt == None)
          this.table += hourCode -> new IntToIntWrapper(1);
        else
          opt.get.asInstanceOf[IntToIntWrapper].incrementBy(1)
      case _ =>
        println("Error - Failed to parse 'minutes:seconds' from :: " + line);
    }
  }

  def flush()
  {
    this.flush(this.table);
  }
}

/**
 * This is similar to feature3, but it outputs the hour of the day and the number of requests,
 * without respect to the date. So, the result is it tells you the cumulative traffic distribution over all days.
 */
class feature3B(uniqueID:String, priority:Int, outputFile:String) extends TopXConsumer(uniqueID, priority, outputFile)
{
  //Here we will do this in a sneaky way, just keep an array of counts, one for each 60 minute window.
  //When the time comes, we'll sort the indices based on the array content, thus getting the hours ordered
  //by traffic.
  val accessesByHour = scala.collection.mutable.Seq.fill(24)(0);
  //Pull the hour directly from the text, this should avoid any timezone nonsense.
  var pattern = new Regex(".+\\[\\d+{2}/\\D{3}/\\d{4}:(\\d{2}):\\d{2}:\\d{2}\\s+-?\\d{3,5}\\].+");

  def consume(host:String, resource:String, bytes:Int, time:Long, response:Int, line:String)
  {
    var hour : Int = -1;
    line match
    {
      case this.pattern(hour) =>
        this.accessesByHour(hour.toInt) = this.accessesByHour(hour.toInt) + 1;
      case _ =>
        println("Error - Failed to parse hour from :: " + line);
    }
  }

  def flush
  {
    val writer : BufferedWriter = this.getWriter;
    val indices = 0 to 23;
    //Here's the tricky bit for this implementation. The indices are sorted by the ordering of the list contents.
    indices.sortWith(this.accessesByHour(_) > this.accessesByHour(_));

    //var index:Int = -1;
    for(index <- indices)
    {
      //Dereference the index through the list to get the appropriate value.
      writer.write(index + "," + this.accessesByHour(index));
      writer.newLine();
    }
    writer.close();
  }
}

/**
 * Once again, we will implement this with a hash table. The keys will be hosts and the values will be 2-tuples
 * with the times of the last two login attempts. We will keep a separate table with blocked hosts and expiration times.
 * If a host shows up in the blocked table, but the expiration has lapsed, that entry gets removed.
 *
 * As with feature1, no attempts will be made to check if IP addresses and host names are the same machines.
 */
class feature4(uniqueID:String, priority:Int, outputFile:String) extends TopXConsumer(uniqueID, priority, outputFile)
{
  var attemptWindow = 20000;//millseconds
  var blockDuration = 20 * 60000;//milliseconds
  //This will be our list of blocked hosts and the expiration times for those blocks.
  val blockedTable = scala.collection.mutable.HashMap.empty[String, Long];
  //This will be a set of 2-tuples that are the times of the previous two failed attempts from a given host.
  val attemptsTable = scala.collection.mutable.HashMap.empty[String, (Long, Long)];

  /**
   * This function will perform a few basic tasks (in order):
   *  1 - Check if host is blocked.
   *  2 - If blocked, check expiration.
   *  3 - If blocked, log the event.
   *  4 - If failed login, check for other failures within window.
   *  5 - If three failures within window, block.
   */
  def consume(host:String, resource:String, bytes:Int, time:Long, response:Int, line:String)
  {
    val opt = this.blockedTable.get(host);
    if (opt != None)
    {
      val blockExpiration = opt.get;
      if (blockExpiration < time)
      {
        //Unblock.
        this.blockedTable.remove(host);
      }
      else
      {
        //Log attempt.
        val writer : BufferedWriter = this.getWriter;
        this.getWriter.write(line);
        this.getWriter.newLine();

        return;
      }
    }

    //Monitor login attempts. Screen for user-induced failures.
    if ((resource == "/login") && (400 <= response) && (response < 500))
    {
      val opt = this.attemptsTable.get(host);
      if (opt == None)
      {
        //We set the first value in the tuple to ensure it doesn't trigger a block, but allows us to guarantee it is a 2-tuple to start.
        this.attemptsTable += host -> (-blockDuration-1, time);
      }
      else
      {
        val attempts = opt.get;
        if (time - attempts._1 <= this.blockDuration)
        {
          //Block
          blockedTable += host -> (time + blockDuration);
        }
        //This is a minor quibble... Do we hold the final, block-inducing attempt, against them for future attempts?
        //In practice, it shouldn't matter, because the duration of the block is much longer than the attempt window.
        this.attemptsTable += host -> (attempts._2, time);//Log attempt.
      }
    }
  }

  def flush
  {
    writer.close();
  }
}

object insightProgrammingChallenge
{
  def main(args: Array[String]):
  Unit =
  {    
    import scala.io.Source
    
    /**
     * For readability, the regular expression is broken into pieces here.
     * Sample data:
     *   xrf.koo.univie.ac.at - - [01/Jul/1995:00:00:09 -0400] "GET /ksc.html HTTP/1.0" 200 7071
     *   199.120.110.21 - - [01/Jul/1995:00:00:09 -0400] "GET /shuttle/missions/sts-73/mission-sts-73.html HTTP/1.0" 200 4085
     *   211.151.91.96 - - [14/Jul/1995:04:09:57 -0400] "GET /shuttle/missions/sts-70/sts-70-patch-small.gif HTTP/1.0" 200 5026
     *   215.145.83.92 - - [01/Jul/1995:00:00:41 -0400] "GET /shuttle/missions/sts-71/movies/movies.html" 200 3089
     */
    val hostPattern = "(\\S+)\\s+-\\s+-\\s+";  //"xrf.koo.univie.ac.at - - " or "199.120.110.21 - - " <-- NOTE: This must match any non-whitespace string, can't assume it has any '.' characters.
    val timestampPattern = "\\[(\\d+{2}/\\D{3}/\\d{4}:\\d{2}:\\d{2}:\\d{2}\\s+-?\\d{3,5})\\]";  //Sample substring: "[01/Jul/1995:00:00:09 "
    /**
     * Most of the seemingly malformed URI entries seem to be from invalid data appended to the request and getting logged.
     * All of the extraneous data should be captured and carried through in the URI processing, as it may be indicative
     * of interesting phenomena. Trying to infer what the correct URI was is probably not a worthwhile endeavor.
     *
     * These may be problematic when measuring the bandwidth associated with various resources.
     *
     * Examples:
     *   204.120.229.63 - - [01/Jul/1995:04:29:05 -0400] "GET /history/history.html                                                 hqpao/hqpao_home.html HTTP/1.0" 200 1502
     *   wsd20.brooks.af.mil - - [28/Jul/1995:11:03:17 -0400] "GET /shuttle/resources/orbiters/columbia.html      WWWSAM.BROOKS.AF.MIL/ HTTP/1.0" 200 6922
     *   ix-sj19-25.ix.netcom.com - - [08/Jul/1995:23:46:35 -0400] "GET /HTTP/1.0 200 OKDate: Sunday, 09-Jul-95 03:42:07 GMTServer: NCSA/1.3MIME-version: 1.0Content-type: text/htmlMIME-Version: 1.0Server: NCSA-Lycos<html HTTP/1.0" 404 -
     */
    //This is going to be as accepting as possible. Ideally, it should mimic how the webserver handles malformed requests.
    val requestPattern = "\\s*\"(?:GET|POST|PUT|HEAD|DELETE|OPTIONS){0,1}\\s*(\\S+).*\"";  //Sample substring: "/ksc.html"
    //val requestPattern = "\\s*\"(?:GET|POST|PUT|HEAD|DELETE|OPTIONS){0,1}\\s*(\\S+)(?:\\s*HTTP/\\d.\\d){0,1}\s*\"";  //Sample substring: "/ksc.html"
    val responseCodePattern = "\\s+(\\d+)\\s+";  //Sample substring: " 200 "
    val bytesPattern = "(-|\\d+)\\s*";  //Sample substring: "7071"

    val pattern = new Regex(hostPattern + timestampPattern +
                            requestPattern + responseCodePattern + bytesPattern);
    //val consumers : Array[Consumer] = {new feature1("feature1", 2)};

    val consumers : ConsumerCollection = new ConsumerCollection("SubscriberHub", 1, null);
    consumers.addConsumer(new feature1("feature1", 2, args(1) + "hosts.txt"));
    consumers.addConsumer(new feature2("feature2", 2, args(1) + "resources.txt"));
    consumers.addConsumer(new feature3("feature3", 2, args(1) + "hours.txt"));
    consumers.addConsumer(new feature3A("feature3A", 3, args(1) + "alignedHours.txt"));
    consumers.addConsumer(new feature3B("feature3B", 3, args(1) + "cumulativeHours.txt"));
    consumers.addConsumer(new feature4("feature4", 1, args(1) + "blocked.txt"));

    //There may be some non-ASCII characters in the log, while only ASCII characters are legal.
    val bufferedSource = Source.fromFile(args(0), "ISO-8859-1");//Tried ASCII and it would choke on the sample input file.
    //It may not be necessary to convert the entire timestamp, but in the interest of being thorough, let's do it right.
    //For the test data set, this doesn't come up. For other data sets, the range of dates may change, so don't be lazy here.
    val dateFormatter = new java.text.SimpleDateFormat("dd/MMM/yyyy:HH:mm:ss Z");
    val pos = new java.text.ParsePosition(0);
    var lineCount:Int = 1;
//import scala.util.control.Breaks._
//{ 
    for (line <- bufferedSource.getLines)
    {
//println(line)
      line match
      {
        case pattern(host, timestamp, request, responseCode, bytes) =>
          //Convert timestamp into millseconds from the beginning of the epoch.
          pos.setIndex(0);
          pos.setErrorIndex(0);
          val time = dateFormatter.parse(timestamp, pos).getTime();

          var byteNum = 0;
          if (bytes != "-")
            byteNum = bytes.toInt;

          //Publish data to subscribers.
          consumers.consume(host, request, byteNum, time, responseCode.toInt, line);

        case _ =>
          println("Error - Failed to parse log entry :: " + line);
      }
      
      lineCount += 1;
      if (lineCount % 10000 == 0)
      {
        //It is comforting to see progress underway, especially when this can take a long time with large files.
        println(lineCount + " lines processed.");
      }
//if (lineCount > 250000)
//{
//  consumers.flush();
//  bufferedSource.close;
//  break;
//}
    }
//}
    println("Finished processing input file.");
    
    //Make sure the results are written to the output files and all files are closed.
    consumers.flush();
    bufferedSource.close;
  }
}