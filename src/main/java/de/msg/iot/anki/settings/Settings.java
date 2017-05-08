package de.msg.iot.anki.settings;


public interface Settings {

    String get(String key);

    default String get(String key, String defaultValue) {
        String value = get(key);
        if(value == null)
            return defaultValue;
        return value;
    }

    default Integer getAsInt(String key, Integer defaultValue) {
        String value = get(key);
        if (value == null)
            return defaultValue;
        try {
            return Integer.parseInt(value);
        } catch (NumberFormatException e) {
            throw new SettingsException("Setting is no valid number [" + value + "].", e);
        }
    }

    default Long getAsLong(String key, Long defaultValue) {
        String value = get(key);
        if (value == null)
            return defaultValue;
        try {
            return Long.parseLong(value);
        } catch (NumberFormatException e) {
            throw new SettingsException("Setting is no valid number [" + value + "].", e);
        }
    }

    default Float getAsFloat(String key, Float defaultValue) {
        String value = get(key);
        if (value == null)
            return defaultValue;
        try {
            return Float.parseFloat(value);
        } catch (NumberFormatException e) {
            throw new SettingsException("Setting is no valid number [" + value + "].", e);
        }
    }

    default Double getAsDouble(String key, Double defaultValue) {
        String value = get(key);
        if (value == null)
            return defaultValue;
        try {
            return Double.parseDouble(value);
        } catch (NumberFormatException e) {
            throw new SettingsException("Setting is no valid number [" + value + "].", e);
        }
    }

    default Boolean getAsBoolean(String key, Boolean defaultValue) {
        String value = get(key);
        if (value == null)
            return defaultValue;
        return Boolean.valueOf(value);
    }

}
