def get_rainfall_emoji(val):
    if val < 1:
        return '☀️'  # No rain
    elif 1 <= val < 10:
        return '🌦️'  # Light rain
    elif 10 <= val < 20:
        return '🌧️'  # Moderate rain
    elif 20 <= val < 50:
        return '🌊'  # Heavy rain
    else:
        return '🌧️🌊'  # Very heavy rain
