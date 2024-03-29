package de.haw.bachelorthesis.dkirchner

/*
 * This file is part of my bachelor thesis.
 *
 * Copyright 2015 Daniel Kirchner <daniel.kirchner1@haw-hamburg.de>
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU Library General Public License as
 * published by the Free Software Foundation; either version 2 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public
 * License along with this program; if not, write to the
 * Free Software Foundation, Inc.,
 * 51 Franklin Street, Fifth Floor, Boston, MA 02110-1301, USA.
 */

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
    var fetchedMessagesCounter = 0

    try {
      store.connect("imap.gmail.com", account, password)
      val inbox = store.getFolder("Inbox/test")
      inbox.open(Folder.READ_WRITE)

      val messages = inbox.getMessages
      var rawText = new String

      // iterate over every retrieved message to extract and concatenate message bodies
      messages.foreach(msg => {
        rawText = ""
        try {

          // iterate over parts of multipart messages and extract text
          if (msg.getContent.isInstanceOf[Multipart]) {
            val multiPartMessage = msg.getContent.asInstanceOf[Multipart]
            for (i <- 0 to multiPartMessage.getCount - 1) {
              if (multiPartMessage.getBodyPart(i).isMimeType("text/plain")) {
                rawText = multiPartMessage.getBodyPart(i).getContent.asInstanceOf[String]
              }
            }
          }

          // directly extract text from plain text messages
          if (msg.getContent.isInstanceOf[String]) {
            rawText = msg.getContent.asInstanceOf[String]
          }

          // append extracted text to a newline-separated message string
          // this also performs basic filtering an normalization on the text
          if (rawText != "") {
            val bodyString = rawText
            val bodyLines = bodyString.split('\n')
              .filter(line => !line.trim.startsWith(">")) // remove quoted lines
              .filter(line => !line.trim.startsWith("<")) // remove tagged lines (html)
              .filter(line => !line.trim.startsWith("On"))
            val cleanLines = bodyLines.map(line => line.stripLineEnd) // remove newlines
            var cleanText: String = null
            if (cleanLines.nonEmpty){
              cleanText = cleanLines.reduce((line1, line2) => line1 + " " + line2).replaceAll("[^a-zA-Z0-9]", " ") // remove special characters
            } else

              fetchedMessagesCounter += 1
            messageTexts.append(cleanText + "\n")
          }
        } catch {
          case uee: UnsupportedEncodingException =>  //discard current data and continue loop
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
    println("Numer of messages read: " + fetchedMessagesCounter)
    return messageTexts.toString()
  }
}