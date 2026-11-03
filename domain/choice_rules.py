"""Accepted text answers and labels for interactive media choices."""

KIND_LABELS = {"music": "🎵 музыку", "movie": "🎬 фильмы"}

KIND_ALIASES = {
    "1": "music", "м": "music", "музыка": "music", "music": "music",
    "2": "movie", "ф": "movie", "фильм": "movie", "видео": "movie", "movie": "movie",
    "0": "cancel", "отмена": "cancel", "нет": "cancel", "cancel": "cancel",
}

VIDEO_KIND_ALIASES = {
    "1": "movie", "ф": "movie", "фильм": "movie", "movie": "movie",
    "2": "series", "с": "series", "сериал": "series", "series": "series",
    "0": "cancel", "отмена": "cancel", "нет": "cancel", "cancel": "cancel",
}

YES_ALIASES = {
    "да": "yes", "д": "yes", "yes": "yes", "y": "yes", "1": "yes",
    "нет": "cancel", "н": "cancel", "no": "cancel", "n": "cancel", "0": "cancel",
    "отмена": "cancel", "cancel": "cancel",
}
