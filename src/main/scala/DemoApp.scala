object DemoApp extends App {
  val appName = "sber1"

  println(s"HELLO\t$appName")

  val json = DemoLogic.process("23") // <-- put itemId here
  println(json)

  println(s"BYE\t$appName")
}
