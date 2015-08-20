package sparky;

import com.esotericsoftware.kryo.Kryo;
import org.apache.spark.serializer.KryoRegistrator;

import javax.annotation.Nonnull;
import java.io.Serializable;
import java.lang.reflect.Array;

public class KryoSerializer implements KryoRegistrator, Serializable {

    public KryoSerializer() {
    }

    /**
     * register a class indicated by name
     */
    protected void doRegistration(@Nonnull Kryo kryo, @Nonnull String s ) {
        Class c;
        try {
            c = Class.forName(s);
            doRegistration(kryo,  c);
        }
        catch (ClassNotFoundException e) {
            return;
        }
    }

    /**
     * register a class
     */
    protected void doRegistration(final Kryo kryo , final Class pC) {
        if (kryo != null) {
            kryo.register(pC);
            // also register arrays of that class
            Class arrayType = Array.newInstance(pC, 0).getClass();
            kryo.register(arrayType);
        }
    }

    /**
     * Real work of registering all classes
     */
    @Override
    public void registerClasses(@Nonnull Kryo kryo) {
        kryo.register(Object[].class);
        kryo.register(scala.Tuple2[].class);
        kryo.register(Model.class);

        doRegistration(kryo, "scala.collection.mutable.WrappedArray$ofRef");
        doRegistration(kryo, "org.systemsbiology.xtandem.scoring.VariableStatistics");
        doRegistration(kryo, "org.systemsbiology.xtandem.scoring.SpectralPeakUsage$PeakUsage");



    }


}