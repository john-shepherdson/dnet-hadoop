package eu.dnetlib.dhp.crossref

import eu.dnetlib.dhp.application.AbstractScalaApplication
import org.slf4j.{Logger, LoggerFactory}

class GenerateCrossrefDataset (propertyPath: String, args: Array[String], log: Logger)
  extends AbstractScalaApplication(propertyPath, args, log: Logger) {
  /** Here all the spark applications runs this method
   * where the whole logic of the spark node is defined
   */
  override def run(): Unit = ???
}


object GenerateCrossrefDataset{
  val log:Logger = LoggerFactory.getLogger(getClass)
  val propertyPath ="/eu/dnetlib/dhp/doiboost/crossref_dump_reader/generate_dataset_params.json"

  def main(args: Array[String]): Unit = {
    new GenerateCrossrefDataset(propertyPath,args, log).initialize().run()
  }
}
