package eu.dnetlib.dhp.provision;

import eu.dnetlib.dhp.provision.scholix.summary.Typology;
import eu.dnetlib.dhp.utils.DHPUtils;
import org.apache.commons.lang3.StringUtils;

public class ProvisionUtil {

  public static final String deletedByInferenceJPATH = "$.dataInfo.deletedbyinference";
  public static final String TARGETJSONPATH = "$.target";
  public static final String SOURCEJSONPATH = "$.source";

  // public static RelatedItemInfo getItemType(final String item, final String idPath) {
  // String targetId = DHPUtils.getJPathString(idPath, item);
  // switch (StringUtils.substringBefore(targetId, "|")) {
  // case "50":
  // return new RelatedItemInfo(null,0,1,0);
  // case "60":
  // return new RelatedItemInfo(null,1,0,0);
  // case "70":
  // return new RelatedItemInfo(null,0,0,1);
  // default:
  // throw new RuntimeException("Unknonw target ID");
  //
  // }
  //
  // }

  public static Boolean isNotDeleted(final String item) {
    return !"true".equalsIgnoreCase(DHPUtils.getJPathString(deletedByInferenceJPATH, item));
  }

  public static Typology getItemTypeFromId(String id) {

    switch (StringUtils.substringBefore(id, "|")) {
      case "50":
        return Typology.publication;
      case "60":
        return Typology.dataset;
      case "70":
        return Typology.unknown;
      default:
        throw new RuntimeException("Unknonw ID type");
    }
  }
}
