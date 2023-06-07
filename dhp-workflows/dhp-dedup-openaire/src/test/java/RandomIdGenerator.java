//
// Source code recreated from a .class file by IntelliJ IDEA
// (powered by FernFlower decompiler)
//
import io.opentelemetry.api.trace.SpanId;
import io.opentelemetry.api.trace.TraceId;
import io.opentelemetry.sdk.internal.RandomSupplier;
import io.opentelemetry.sdk.trace.IdGenerator;

import java.util.Random;
import java.util.function.Supplier;

enum RandomIdGenerator implements IdGenerator {
    INSTANCE;

    private static final long INVALID_ID = 0L;
    private static final Supplier<Random> randomSupplier = RandomSupplier.platformDefault();

    private RandomIdGenerator() {
    }

    public String generateSpanId() {
        Random random = (Random)randomSupplier.get();

        long id;
        do {
            id = random.nextLong();
        } while(id == 0L);

        return SpanId.fromLong(id);
    }

    public String generateTraceId() {
        Random random = (Random)randomSupplier.get();
        long idHi = random.nextLong();

        long idLo;
        do {
            idLo = random.nextLong();
        } while(idLo == 0L);

        return TraceId.fromLongs(idHi, idLo);
    }

    public String toString() {
        return "RandomIdGenerator{}";
    }
}
