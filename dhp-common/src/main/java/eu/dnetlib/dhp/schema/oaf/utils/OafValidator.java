package eu.dnetlib.dhp.schema.oaf.utils;

import org.hibernate.validator.messageinterpolation.ParameterMessageInterpolator;

import javax.validation.Validation;
import javax.validation.Validator;
import java.util.HashSet;
import java.util.Set;
import java.util.stream.Collectors;

public class OafValidator {

	private static final Validator VALIDATOR = Validation
		.byDefaultProvider()
		.configure()
		.messageInterpolator(new ParameterMessageInterpolator())
		.buildValidatorFactory()
		.getValidator();

	public static <T> Set<ConstraintViolation> validate(final T oaf) {
		return VALIDATOR.validate(oaf)
				.stream()
				.map(v -> {
					final String path = v.getRootBeanClass().getSimpleName() + "." + v.getPropertyPath().toString();
					ConstraintViolation cv = new ConstraintViolation();
					cv.add(path, v.getMessage());
					return cv;
				})
				.collect(Collectors.toCollection(HashSet::new));
	}

}
