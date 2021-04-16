package zio

import zio.schema.Schema

import java.time._
import java.time.temporal.ChronoUnit
import scala.language.implicitConversions

package object flow {
  type ActivityError
  type Variable[A]

  type RemoteDuration    = Remote[Duration]
  type RemoteInstant     = Remote[Instant]
  type RemoteVariable[A] = Remote[Variable[A]]

  implicit val schemaChronoUnit: Schema[ChronoUnit] = Schema[String].transformOrFail({
    case "SECONDS" => Right(ChronoUnit.SECONDS)
    case _ => Left("Failed")
  }, {
    //TODO : Add the rest
    case ChronoUnit.SECONDS => Right("SECONDS")
    case _ => Left("Failed")
  })

  implicit val schemaNil: Schema[Nil.type]                                         = Schema[Unit].transform(_ => Nil, _ => ())
  implicit def schemaLeft[A,B](implicit schema: Schema[A]): Schema[Left[A,B]] =
    schema.transform(a => Left(a), leftA => leftA.value)

  implicit def schemaRight[A, B](implicit schema: Schema[A]): Schema[Right[B, A]] =
    schema.transform(a => Right(a), rightA => rightA.value)

  implicit def RemoteVariable[A](remote: Remote[Variable[A]]): RemoteVariableSyntax[A] = new RemoteVariableSyntax(
    remote
  )

  implicit val schemaNone: Schema[None.type] = Schema[Unit].transform(_ => None, _ => ())

  implicit def RemoteInstant(remote: Remote[Instant]): RemoteInstantSyntax = new RemoteInstantSyntax(
    remote
  )

  implicit def RemoteDuration(remote: Remote[Duration]): RemoteDurationSyntax = new RemoteDurationSyntax(
    remote
  )
}
