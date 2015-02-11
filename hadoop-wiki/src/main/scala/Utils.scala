import java.util.function.Consumer
import java.util.regex.Matcher

import org.apache.hadoop.fs.Path
import org.apache.hadoop.io.{BooleanWritable, FloatWritable, IntWritable, LongWritable, Text, UTF8}
import org.apache.mahout.math.{VarIntWritable, RandomAccessSparseVector, VectorWritable}

import scala.collection.immutable.IndexedSeq

object Utils {

  implicit def writable2boolean(value: BooleanWritable) = value.get

  implicit def boolean2writable(value: Boolean) = new BooleanWritable(value)

  implicit def writable2int(value: IntWritable) = value.get

  implicit def int2writable(value: Int) = new IntWritable(value)

  implicit def int2Varwritable(value: Int) = new VarIntWritable(value)

  implicit def writable2long(value: LongWritable) = value.get

  implicit def long2writable(value: Long) = new LongWritable(value)

  implicit def writable2float(value: FloatWritable) = value.get

  implicit def float2writable(value: Float) = new FloatWritable(value)

  implicit def text2string(value: Text) = value.toString

  implicit def string2text(value: String) = new Text(value)

  implicit def uft82string(value: UTF8) = value.toString

  implicit def string2utf8(value: String) = new UTF8(value)

  implicit def path2string(value: Path) = value.toString

  implicit def string2path(value: String) = new Path(value)

  implicit def javaIterator2Iterator[A](value: java.util.Iterator[A]) = new Iterator[A] {
    def hasNext = value.hasNext

    def next = value.next
  }

  implicit def javaIterator2BooleanIterator(value: java.util.Iterator[BooleanWritable]) = new Iterator[Boolean] {
    def hasNext = value.hasNext

    def next = value.next.get
  }

  implicit def javaIterator2IntIterator(value: java.util.Iterator[IntWritable]) = new Iterator[Int] {
    def hasNext = value.hasNext

    def next = value.next.get
  }

  implicit def javaIterator2LongIterator(value: java.util.Iterator[LongWritable]) = new Iterator[Long] {
    def hasNext = value.hasNext

    def next = value.next.get
  }

  implicit def javaIterator2FloatIterator(value: java.util.Iterator[FloatWritable]) = new Iterator[Float] {
    def hasNext = value.hasNext

    def next = value.next.get
  }

  implicit def javaIterator2TextIterator(value: java.util.Iterator[Text]) = new Iterator[String] {
    def hasNext = value.hasNext

    def next = value.next.toString
  }

  implicit def javaIterator2UTF8Iterator(value: java.util.Iterator[UTF8]) = new Iterator[String] {
    def hasNext = value.hasNext
    def next = value.next.toString
  }

  implicit def randomAccessSparseVector2VectorWritable(v: RandomAccessSparseVector) = new VectorWritable(v)

  implicit def function2Consumer[T](f: Function[T, Unit]) = {
    new Consumer[T] {
      override def accept(t: T) = f(t)
    }
  }

  implicit def matcher2Iterator(m: Matcher) = new Iterator[String]{
    def hasNext = m find
    def next = m group
  }
}