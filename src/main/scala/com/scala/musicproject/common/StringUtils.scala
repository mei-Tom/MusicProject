package com.scala.musicproject.common

object StringUtils {

  def checkString(str:String) = {
    if(str == null || "".equals(str)) "" else str
  }




}
