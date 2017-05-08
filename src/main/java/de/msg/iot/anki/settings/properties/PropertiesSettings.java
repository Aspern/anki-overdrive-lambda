package de.msg.iot.anki.settings.properties;

import de.msg.iot.anki.settings.Settings;
import de.msg.iot.anki.settings.SettingsException;

import java.io.FileInputStream;
import java.io.InputStream;
import java.util.Properties;

public class PropertiesSettings implements Settings {

    private final Properties properties;

    public PropertiesSettings(Properties properties) {
        this.properties = properties;
    }

    public PropertiesSettings(String resource) {
        this(new Properties());

        InputStream in = null;

        try {
            in = getClass().getClassLoader().getResourceAsStream(resource);
            if (in == null) {
                in = new FileInputStream("src/main/resources/" + resource);
            }
            this.properties.load(in);
        } catch (Exception e) {
            throw new SettingsException("Cannot load properties [" + resource + "].", e);
        } finally {
            if (in != null)
                try {
                    in.close();
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }

        }
    }

    @Override
    public String get(String key) {
        return this.properties.getProperty(key);
    }
}
