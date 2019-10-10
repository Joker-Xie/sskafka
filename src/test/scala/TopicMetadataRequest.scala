case class TopicMetadataRequest(val versionId: Short,
                                val correlationId: Int,
                                val clientId: String,
                                val topics: Seq[String])
