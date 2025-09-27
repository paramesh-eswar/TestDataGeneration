package org.pgs.app;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;

import javax.swing.SwingUtilities;

import org.junit.jupiter.api.Test;

public class TestDataGeneratorUITest {

    @Test
    public void createAndDisposeUiOnEdt() throws Exception {
        assertDoesNotThrow(() -> {
            Runnable r = () -> {
                TestDataGeneratorUI ui = new TestDataGeneratorUI();
                ui.setVisible(false);
                ui.dispose();
            };
            if (SwingUtilities.isEventDispatchThread()) {
                r.run();
            } else {
                SwingUtilities.invokeAndWait(r);
            }
        });
    }
}
