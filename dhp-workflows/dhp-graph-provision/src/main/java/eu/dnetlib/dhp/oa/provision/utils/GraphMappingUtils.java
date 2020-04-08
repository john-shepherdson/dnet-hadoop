package eu.dnetlib.dhp.oa.provision.utils;

import com.google.common.collect.Sets;
import eu.dnetlib.dhp.oa.provision.model.RelatedEntity;
import eu.dnetlib.dhp.schema.common.EntityType;
import eu.dnetlib.dhp.schema.oaf.*;

import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

import static org.apache.commons.lang3.StringUtils.substringAfter;

public class GraphMappingUtils {

    public static final String SEPARATOR = "_";

    public static Set<String> authorPidTypes = Sets.newHashSet("orcid", "magidentifier");

    public static <E extends OafEntity> RelatedEntity asRelatedEntity(E entity, Class<E> clazz) {

        final RelatedEntity re = new RelatedEntity();
        re.setId(entity.getId());
        re.setType(EntityType.fromClass(clazz).name());

        re.setPid(entity.getPid());
        re.setCollectedfrom(entity.getCollectedfrom());

        switch (EntityType.fromClass(clazz)) {
            case publication:
            case dataset:
            case otherresearchproduct:
            case software:

                Result result = (Result) entity;

                if (result.getTitle() == null && !result.getTitle().isEmpty()) {
                    re.setTitle(result.getTitle().stream().findFirst().get());
                }

                re.setDateofacceptance(getValue(result.getDateofacceptance()));
                re.setPublisher(getValue(result.getPublisher()));
                re.setResulttype(result.getResulttype());
                re.setInstances(result.getInstance());

                //TODO still to be mapped
                //re.setCodeRepositoryUrl(j.read("$.coderepositoryurl"));

                break;
            case datasource:
                Datasource d = (Datasource) entity;

                re.setOfficialname(getValue(d.getOfficialname()));
                re.setWebsiteurl(getValue(d.getWebsiteurl()));
                re.setDatasourcetype(d.getDatasourcetype());
                re.setOpenairecompatibility(d.getOpenairecompatibility());

                break;
            case organization:
                Organization o = (Organization) entity;

                re.setLegalname(getValue(o.getLegalname()));
                re.setLegalshortname(getValue(o.getLegalshortname()));
                re.setCountry(o.getCountry());
                re.setWebsiteurl(getValue(o.getWebsiteurl()));
                break;
            case project:
                Project p = (Project) entity;

                re.setProjectTitle(getValue(p.getTitle()));
                re.setCode(getValue(p.getCode()));
                re.setAcronym(getValue(p.getAcronym()));
                re.setContracttype(p.getContracttype());

                List<Field<String>> f = p.getFundingtree();
                if (!f.isEmpty()) {
                    re.setFundingtree(f.stream()
                            .map(s -> s.getValue())
                            .collect(Collectors.toList()));
                }
                break;
        }
        return re;
    }

    private static String getValue(Field<String> field) {
        return getFieldValueWithDefault(field, "");
    }

    private static <T> T getFieldValueWithDefault(Field<T> f, T defaultValue) {
        return Optional.ofNullable(f)
                .filter(Objects::nonNull)
                .map(x -> x.getValue())
                .orElse(defaultValue);
    }

    public static String removePrefix(final String s) {
        if (s.contains("|")) return substringAfter(s, "|");
        return s;
    }

    public static String getRelDescriptor(String relType, String subRelType, String relClass) {
        return relType + SEPARATOR + subRelType + SEPARATOR + relClass;
    }

}
