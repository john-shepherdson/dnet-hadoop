package eu.dnetlib.dhp.bypassactionset;

import eu.dnetlib.dhp.schema.oaf.utils.CleaningFunctions;
import eu.dnetlib.dhp.schema.oaf.utils.IdentifierFactory;
import org.jetbrains.annotations.NotNull;

public class Utils {
    private static final String ID_PREFIX = "50|doi_________";

    @NotNull
    public static String getIdentifier(String d) {
        return ID_PREFIX +
                IdentifierFactory.md5(CleaningFunctions.normalizePidValue("doi", d));
    }
}
