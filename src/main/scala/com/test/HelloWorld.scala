package com.test

object HelloWorld {
    def main(args: Array[String]): Unit = {
        print("Hello")
        val word = "Hello World"
        println(word)
        println(word.indexOf("abc"))
        println(word.contains("he"))
        println(word.contains("World"))
        println(word.matches("Wor")) // 不能这么使用

        val reg = "Wor".r
        println(reg.findFirstIn(word))

        val reg1 = "He(.*)ld".r
        println(reg1.findAllIn(word))
        println((reg1 findAllIn word).mkString(","))

        reg1.findAllMatchIn(word).foreach(x => {
            println(x.group(1)); println(x.getClass)
        })
        println(reg1.findFirstMatchIn(word).iterator.next().group(1))

        println(addInt(5,6))
    }

    def addInt( a:Int, b:Int ) : Int = {
        var sum:Int = 0
        sum = a + b

        return sum  // scala 可以没有return？
    }
}
