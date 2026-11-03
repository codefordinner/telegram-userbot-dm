"""Repair MP3 ID3 tags for consistent Unicode and library compatibility."""

import os
import logging

from domain.filename_utils import fix_mojibake
from mutagen.id3 import ID3, ID3NoHeaderError

log = logging.getLogger("userbot-dm-uploader")


def _has_id3v1(path: str) -> bool:
    try:
        with open(path, "rb") as f:
            f.seek(-128, os.SEEK_END)
            return f.read(3) == b"TAG"
    except OSError:
        return False


def fix_mp3_tags(path: str) -> None:
    """Приводит теги mp3 в порядок (синхронная; вызывать через run_blocking):

    1. убирает мусорные ID3-фреймы (пустые USLT/UFID от pesni.fm), которые ломают tagr (node-taglib-sharp);
    2. чинит «кракозябры» в тексте тегов (cp1251/UTF-8, прочитанные как Latin-1);
    3. явно помечает не-ASCII текст как Unicode (UTF-16 для ID3v2.3, UTF-8 для v2.4). Теги с кириллицей
       в «Latin-1» плееры (особенно Android) угадывают сами — так и появляются иероглифы;
    4. удаляет ID3v1 — он не умеет Unicode, а часть плееров предпочитает его ID3v2.
    """
    try:
        tag = ID3(path)
    except ID3NoHeaderError:
        return  # тегов нет вообще — чинить нечего
    except Exception as e:
        # Битые/нестандартные mp3 могут кидать не только MutagenError
        # (KeyError, struct.error и т.п.) — это не критичный путь, просто пропускаем.
        log.warning("Не удалось прочитать ID3-теги для очистки: %s: %s", path, e)
        return

    target_v23 = tuple(tag.version[:2]) == (2, 3)
    unicode_encoding = 1 if target_v23 else 3  # 1 = UTF-16, 3 = UTF-8
    changed = False

    for key in list(tag.keys()):
        frame = tag[key]

        if key.startswith("USLT") and (not frame.text or getattr(frame, "lang", "") == "XXX"):
            del tag[key]
            changed = True
            continue
        if key.startswith("UFID") and not frame.data:
            del tag[key]
            changed = True
            continue

        text = getattr(frame, "text", None)
        if isinstance(text, str):
            new_text, texts = fix_mojibake(text), None
            texts = [new_text]
        elif isinstance(text, list) and all(isinstance(t, str) for t in text):
            new_text = [fix_mojibake(t) for t in text]
            texts = new_text
        else:
            continue

        if new_text != text:
            log.info("ID3 %s: %r -> %r", key, text, new_text)
            frame.text = new_text
            changed = True

        encoding = getattr(frame, "encoding", None)
        if encoding is not None and any(not t.isascii() for t in texts):
            if (target_v23 and encoding != 1) or (not target_v23 and encoding == 0):
                frame.encoding = unicode_encoding
                changed = True

    if _has_id3v1(path):
        changed = True
    if not changed:
        return

    try:
        if target_v23:
            tag.update_to_v23()
        tag.save(path, v1=0, v2_version=3 if target_v23 else 4)
        log.info("Теги приведены в порядок: %s", path)
    except Exception as e:
        log.warning("Не удалось сохранить очищенные ID3-теги: %s: %s", path, e)


