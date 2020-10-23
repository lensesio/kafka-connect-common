package com.datamountaineer.streamreactor.connect.schemas

import org.apache.kafka.connect.data.Struct

import scala.collection.JavaConverters._

object StructHelper {

  implicit final class StructExtension(val struct: Struct) extends AnyVal {
    def extractValueFromPath(path: String): Either[FieldValueExtractionError, Option[AnyRef]] = {
      val fields = path.split('.')
      val start: Either[FieldValueExtractionError, State] = Right(State(Some(struct), Vector.empty))

      fields.foldLeft(start) {
        case (l@Left(_), _) => l
        case (s@Right(state), field) =>
          state.value.fold(s.asInstanceOf[Either[FieldValueExtractionError, State]]) {
            case s: Struct =>
              s.fieldValue(field) match {
                case Some(value) =>
                  Right.apply[FieldValueExtractionError, State](state.copy(value = Some(value), path = state.path :+ field))
                case None =>
                  val path = (state.path :+ field).mkString(".")
                  val msg = s"Field [$path] does not exist. Available Fields are [${s.schema().fields().asScala.map(_.name()).mkString(",")}]"
                  Left.apply[FieldValueExtractionError, State](FieldValueExtractionError(path, msg))
              }
            case other =>
              val path = state.path.mkString(".")
              Left.apply[FieldValueExtractionError, State](FieldValueExtractionError(path, s"Expecting a a structure but found [$other]."))
          }
      }
        .map(_.value)
    }

    def fieldValue(field: String): Option[AnyRef] = {
      Option(struct.schema().field(field)).map { _ =>
        struct.get(field)
      }
    }
  }

  private final case class State(value: Option[AnyRef], path: Vector[String])

}

case class FieldValueExtractionError(path: String, msg: String)
