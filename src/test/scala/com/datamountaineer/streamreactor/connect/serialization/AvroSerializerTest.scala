package com.datamountaineer.streamreactor.connect.serialization

import java.io.{ByteArrayInputStream, ByteArrayOutputStream}

import org.scalatest.{Matchers, WordSpec}

class AvroSerializerTest extends WordSpec with Matchers {
  "AvroSerializer" should {
    "read and write from and to Avro" in {
      val book = Book("On Intelligence", Author("Jeff", "Hawkins", 1957), "0805078533", 273, 14.72)

      implicit  val os = new ByteArrayOutputStream()
      AvroSerializer.write(book)

      implicit val is = new ByteArrayInputStream(os.toByteArray)

      val actualBook = AvroSerializer.read[Book](is)

      actualBook shouldBe book
      os.toByteArray shouldBe AvroSerializer.getBytes(book)
    }
  }


  case class Author(firstName: String, lastName: String, yearBorn: Int)

  case class Book(title: String, autor: Author, isbn: String, pages: Int, price: Double)

}
