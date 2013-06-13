package org.mkleine

import com.coremedia.cap.Cap

object CMSegmentGenerator {

  var segmentCount = 0

  def main(args: Array[String]) {
    val url = if (args.length > 0) args(0) else "http://localhost:41080/coremedia/ior"
    val pwd = if (args.length > 1) args(1) else ""
    var id = if (args.length > 2) args(2).toInt else 0
    val limit = if (args.length > 3) args(3).toInt else 100000

    println("opening connection ...")
    val connection = Cap.connect(url, "admin", pwd)
    println("connection open")
    val repo = connection.getContentRepository
    val queryService = repo.getQueryService
    val reader = new CMTaxonomyReader(repo, queryService)

    while (id < limit ) {
      println("starting with id " + id)
      id = reader.readTaxonomies(id)
    }

    println("created " + segmentCount + " segments")

    exit(0)
  }

}

import com.coremedia.cap.content.query.QueryService
import com.coremedia.cap.content._
import com.coremedia.cap.common.IdHelper
import com.coremedia.xml.MarkupFactory
import collection.JavaConversions._
import collection.immutable._

class CMTaxonomyReader(val repo: ContentRepository, val queryService: QueryService) {

  val publisher = repo.getPublicationService

  def readTaxonomies(id:Int) =  {
    val taxonomies:scala.collection.Iterable[Content] = queryService.poseContentQuery("TYPE = CMTaxonomy AND id > ?0 ORDER BY id LIMIT 1000", id.asInstanceOf[Object])
    val segments = taxonomies map createSegment
    println("got segments " + segments + " for taxonomies " + taxonomies + " with IDs greater than " + id)
    publisher.publish(segments)
    CMSegmentGenerator.segmentCount += segments.size

    if(taxonomies.isEmpty) (id + 1000) else IdHelper.parseContentId(taxonomies.last.getId)
  }

  def createSegment(taxonomy:Content) = {
    val id = taxonomy.getId
    val path = "/System/personalization/segments/"+ taxonomy.getName + "-" + IdHelper.parseContentId(id) + "-" +System.currentTimeMillis
    println("creating segment " + path)
    val condition = MarkupFactory.fromString("<rules version=\"1.0\" xmlns=\"http://www.coremedia.com/2010/selectionrules\" xmlns:xlink=\"http://www.w3.org/1999/xlink\">" +
                                                                             "subjectTaxonomies.<content xlink:href=\"" + id + "\"/>&gt;0.05 or " +
                                                                             "locationTaxonomies.<content xlink:href=\"" + id + "\"/>&gt;0.05 or " +
                                                                             "explicit.<content xlink:href=\"" + id + "\"/>&gt;0.05" +
                                                                           "</rules>")
    val segment = repo.createChild(path,"CMSegment",Map[String,Object]("conditions" -> condition))
    publisher.approve(segment.checkIn)
    publisher.approvePlace(segment)
    segment
  }

}