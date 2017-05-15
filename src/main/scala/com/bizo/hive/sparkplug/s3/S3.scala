package com.bizo.hive.sparkplug.s3

import com.amazonaws.services.s3.{AmazonS3, AmazonS3ClientBuilder}
import java.io.File

class S3(s3: AmazonS3) {
  import S3._
  
  def cp(file: File, s3Path: String) {
    val (bucket, key) = parseS3Url(s3Path)
    
    s3.putObject(bucket, key, file)
  }
}

object S3 {
  lazy val defaultS3 = new S3(AmazonS3ClientBuilder.defaultClient())
  
  def s3cp(file: File, s3Path: String) {
    defaultS3.cp(file, s3Path)
  }
  
  implicit def toRichFile(f: File) = new RichFile(f)
  implicit def toFile(r: RichFile) = r.f  
  
  private val s3UrlPattern = """s3://([^/]+)/(.+)""".r
  private val s3BucketKeyPattern = """([^:]+):(.+)""".r
  
  def parseS3Url(s3Path: String): (String, String) = s3Path match {
    case s3UrlPattern(bucket, key) => (bucket, key)
    case s3BucketKeyPattern(bucket, key) => (bucket, key)
    case _ => sys.error("Expecting s3://bucket/key or bucket:key urls, can't parse: " + s3Path)
  }  
}

class RichFile(val f: File) {
  def `..`: RichFile = new RichFile(f.getParentFile())
  def /(path: String): RichFile = new RichFile(new File(f, path))
}