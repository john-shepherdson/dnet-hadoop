package eu.dnetlib.dhp.schema.oaf.utils;

import java.lang.reflect.Field;
import java.util.Collection;
import java.util.Iterator;

public class MergeUtils2 {

    /**
     * Recursively merges the fields of the provider into the receiver.
     *
     * @param receiver the receiver instance.
     * @param provider the provider instance.
     */
    public static <T> void merge(final T receiver, final T provider) {
        Field[] fields = receiver.getClass().getDeclaredFields();
        for (Field field : fields) {

            try {
                field.setAccessible(true);
                Object receiverObject = field.get(receiver);
                Object providerObject = field.get(provider);

                if (receiverObject == null || providerObject == null) {
                    /* One is null */

                    field.set(receiver, providerObject);
                } else if (field.getType().isAssignableFrom(Collection.class)) {
                    /* Collection field */
                    // noinspection rawtypes
                    mergeCollections((Collection) receiverObject, (Collection) providerObject);
                } else if (field.getType().isPrimitive() || field.getType().isEnum()
                        || field.getType().equals(String.class)) {
                    /* Primitive, Enum or String field */
                    field.set(receiver, providerObject);
                } else {
                    /* Mergeable field */
                    merge(receiverObject, providerObject);
                }
            } catch (IllegalAccessException e) {
                /* Should not happen */
                throw new RuntimeException(e);
            }
        }
    }

    /**
     * Recursively merges the items in the providers collection into the receivers collection.
     * Receivers not present in providers will be removed, providers not present in receivers will be added.
     * If the item has a field called 'id', this field will be compared to match the items.
     *
     * @param receivers the collection containing the receiver instances.
     * @param providers the collection containing the provider instances.
     */
    public static <T> void mergeCollections(final Collection<T> receivers, final Collection<T> providers) {
        if (receivers.isEmpty() && providers.isEmpty()) {
            return;
        }

        if (providers.isEmpty()) {
            receivers.clear();
            return;
        }

        if (receivers.isEmpty()) {
            receivers.addAll(providers);
            return;
        }

        Field idField;
        try {
            T t = providers.iterator().next();
            idField = t.getClass().getDeclaredField("id");
            idField.setAccessible(true);
        } catch (NoSuchFieldException ignored) {
            idField = null;
        }

        try {
            if (idField != null) {
                mergeCollectionsWithId(receivers, providers, idField);
            } else {
                mergeCollectionsSimple(receivers, providers);
            }
        } catch (IllegalAccessException e) {
            /* Should not happen */
            throw new RuntimeException(e);
        }
    }

    /**
     * Recursively merges the items in the collections for which the id's are equal.
     *
     * @param receivers the collection containing the receiver items.
     * @param providers the collection containing the provider items.
     * @param idField the id field.
     *
     * @throws IllegalAccessException if the id field is not accessible.
     */
    private static <T> void mergeCollectionsWithId(final Collection<T> receivers, final Iterable<T> providers,
                                                   final Field idField) throws IllegalAccessException {
        /* Find a receiver for each provider */
        for (T provider : providers) {
            boolean found = false;
            for (T receiver : receivers) {
                if (idField.get(receiver).equals(idField.get(provider))) {
                    merge(receiver, provider);
                    found = true;
                }
            }
            if (!found) {
                receivers.add(provider);
            }
        }

        /* Remove receivers not in providers */
        for (Iterator<T> iterator = receivers.iterator(); iterator.hasNext();) {
            T receiver = iterator.next();
            boolean found = false;
            for (T provider : providers) {
                if (idField.get(receiver).equals(idField.get(provider))) {
                    found = true;
                }
            }
            if (!found) {
                iterator.remove();
            }
        }
    }

    /**
     * Recursively merges the items in the collections one by one. Disregards equality.
     *
     * @param receivers the collection containing the receiver items.
     * @param providers the collection containing the provider items.
     */
    private static <T> void mergeCollectionsSimple(final Collection<T> receivers, final Iterable<T> providers) {
        Iterator<T> receiversIterator = receivers.iterator();
        Iterator<T> providersIterator = providers.iterator();
        while (receiversIterator.hasNext() && providersIterator.hasNext()) {
            merge(receiversIterator.next(), providersIterator.next());
        }

        /* Remove excessive receivers if present */
        while (receiversIterator.hasNext()) {
            receiversIterator.next();
            receiversIterator.remove();
        }

        /* Add residual providers to receivers if present */
        while (providersIterator.hasNext()) {
            receivers.add(providersIterator.next());
        }
    }

}
