package de.haw.bachelorthesis.dkirchner

import java.io.UnsupportedEncodingException
import javax.mail._

/**
 * Created by Daniel on 25.05.2015.
 */
object MailService {
  def fetchFrom(account: String, password: String): String = {
    val props = System.getProperties
    props.setProperty("mail.store.protocol", "imaps")
    val session = Session.getDefaultInstance(props, null)
    val store = session.getStore("imaps")
    val messageTexts: StringBuilder = new StringBuilder

    try {
      store.connect("imap.gmail.com", account, password)
      val inbox = store.getFolder("Inbox")
      inbox.open(Folder.READ_WRITE)

      val messages = inbox.getMessages
      var rawText = new String
      var counter = 0

      messages.foreach(msg => {
        rawText = ""
        try {
          if (msg.getContent.isInstanceOf[Multipart]) {
            val multiPartMessage = msg.getContent.asInstanceOf[Multipart]
            for (i <- 0 to multiPartMessage.getCount - 1) {
              if (multiPartMessage.getBodyPart(i).getContent.isInstanceOf[String]) {
                val rawText = multiPartMessage.getBodyPart(i).getContent.asInstanceOf[String]
              }
            }
          }


          if (msg.getContent.isInstanceOf[String]) {
            rawText = msg.getContent.asInstanceOf[String]
          }


          if (rawText != "") {
            val bodyString = rawText
            val bodyLines = bodyString.split('\n')
              .filter(line => !line.trim.startsWith(">")) // remove quoted lines
              .filter(line => !line.trim.startsWith("<")) // remove html tags
              .filter(line => !line.trim.startsWith("On"))
            val cleanLines = bodyLines.map(line => line.stripLineEnd) // remove newlines
            val cleanText = if (cleanLines.nonEmpty)
                cleanLines.reduce(_ + _).replaceAll("[^a-zA-Z0-9]", " ") // remove special characters
              else ""

            counter += 1
            println( counter.toString )
            messageTexts.append(cleanText + "\n")
          }
        } catch {
          case uee: UnsupportedEncodingException =>  //continue
        }
      })

      inbox.close(true)
    } catch {
      case e: NoSuchProviderException => e.printStackTrace()
        System.exit(1)
      case me: MessagingException     => me.printStackTrace()
        System.exit(2)
    } finally {
      store.close()
    }
    messageTexts.toString()
  }
}
