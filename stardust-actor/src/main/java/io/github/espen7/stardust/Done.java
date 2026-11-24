package io.github.espen7.stardust;

import java.io.Serializable;

/** CoordinatedShutdown 成功完成时的结果。 */
public final class Done implements Serializable {
    private static final Done INSTANCE = new Done();

    private Done() {
    }

    /** @return Done singleton */
    public static Done getInstance() {
        return INSTANCE;
    }

    /** @return Done singleton */
    public static Done done() {
        return INSTANCE;
    }
}
