"""Plan media names, kinds, series folders, and subtitle targets."""

from pathlib import Path
from typing import Optional


class MediaPlanningService:
    def __init__(
        self, *, caption_naming, kind_labels, kind_aliases, video_kind_aliases,
        ask_choice, ask_text, detect_series, is_generic_name, parse_episode_from_name,
        normalize_text, sanitize_name, truncate_filename, clean_show_name,
        build_series_filename, split_sub_name, pick_episode_video,
        pick_movie_video, copyparty, short_name,
    ):
        self.caption_naming = caption_naming
        self.kind_labels = kind_labels
        self.kind_aliases = kind_aliases
        self.video_kind_aliases = video_kind_aliases
        self.ask_choice = ask_choice
        self.ask_text = ask_text
        self.detect_series = detect_series
        self.is_generic_name = is_generic_name
        self.parse_episode_from_name = parse_episode_from_name
        self.normalize_text = normalize_text
        self.sanitize_name = sanitize_name
        self.truncate_filename = truncate_filename
        self.clean_show_name = clean_show_name
        self.build_series_filename = build_series_filename
        self.split_sub_name = split_sub_name
        self.pick_episode_video = pick_episode_video
        self.pick_movie_video = pick_movie_video
        self.copyparty = copyparty
        self.short_name = short_name

    def _kind_note(self, choice: Optional[str]) -> str:
        if choice in self.kind_labels:
            return f"✅ Понял, кладу в {self.kind_labels[choice]}."
        if choice is None:
            return "⌛ Ответа не дождался — файл пропущен."
        return "✖️ Отменено."

    @staticmethod
    def _video_kind_note(choice: Optional[str]) -> str:
        if choice == "movie":
            return "✅ Понял: это фильм."
        if choice == "series":
            return "✅ Понял: это серия сериала."
        if choice is None:
            return "⌛ Ответа не дождался — файл пропущен."
        return "✖️ Отменено."

    async def ask_kind(self, event, filename: str) -> Optional[str]:
        """Ask whether an unknown document should be music or a movie."""
        choice = await self.ask_choice(
            event,
            f"🤔 Не понимаю, что это за файл: «{filename}».\nКуда его положить?",
            [[("🎵 Музыка", "music"), ("🎬 Фильм", "movie")], [("✖️ Отмена", "cancel")]],
            self.kind_aliases,
            "1 — музыка, 2 — фильм, 0 — отмена",
            self._kind_note,
        )
        return choice if choice in self.kind_labels else None

    async def plan_video(self, event, orig_filename: str, ext: str, parsed: Optional[dict], filename: str):
        """Resolve a video as a movie or a series episode, asking when its name is unclear."""
        info = self.detect_series(orig_filename, parsed, self.caption_naming)
        if info is None and self.is_generic_name(Path(filename).stem):
            choice = await self.ask_choice(
                event,
                f"🤔 Не понимаю, что за видео: «{orig_filename}».\nЭто фильм или серия сериала?",
                [[("🎬 Фильм", "movie"), ("📺 Сериал", "series")], [("✖️ Отмена", "cancel")]],
                self.video_kind_aliases,
                "1 — фильм, 2 — сериал, 0 — отмена",
                self._video_kind_note,
            )
            if choice == "movie":
                text = await self.ask_text(
                    event, f"🎬 Как называется фильм? (файл «{orig_filename}»)",
                    "название, лучше с годом — например: Dune (2021)",
                )
                name = self.sanitize_name(text or "")
                if not name:
                    return None
                return None, self.truncate_filename(name + ext)
            if choice == "series":
                text = await self.ask_text(
                    event, f"📺 Что это за серия? (файл «{orig_filename}»)",
                    "название, сезон и серия — например: Dune Prophecy S01E02",
                )
                parsed_answer = self.parse_episode_from_name(self.normalize_text(text or ""))
                if not parsed_answer or not parsed_answer["show"] or self.is_generic_name(parsed_answer["show"]):
                    if text:
                        await event.reply("🤷 Не разобрал название, сезон и серию (нужен вид «Название S01E02») — файл пропущен.")
                    return None
                info = {
                    "show": parsed_answer["show"], "season": parsed_answer["season"],
                    "episode": parsed_answer["episode"], "episode_title": None,
                    "quality": None, "name_ok": False,
                }
            else:
                return None

        if info is None:
            return None, filename
        show = info["show"]
        if not show or self.is_generic_name(show):
            text = await self.ask_text(
                event,
                f"📺 «{orig_filename}» — серия сериала (сезон {info['season']}, серия {info['episode']}), "
                f"но название сериала я не нашёл.",
                "название сериала — например: Dune Prophecy",
            )
            show = self.clean_show_name(text or "")
            if not show or self.is_generic_name(show):
                if text:
                    await event.reply("🤷 Не разобрал название сериала — файл пропущен.")
                return None
            info["name_ok"] = False

        info["show"] = show
        leaf = orig_filename if info["name_ok"] else self.build_series_filename(info, ext)
        return {"show": show, "season": info["season"], "episode": info["episode"]}, self.truncate_filename(leaf)

    async def plan_subtitle(self, event, orig_filename: str) -> tuple:
        """Infer a subtitle's series metadata, preserving the existing ask/cancel behavior."""
        base, _tags = self.split_sub_name(Path(orig_filename).stem)
        info = self.parse_episode_from_name(base)
        if info is None:
            return True, None
        show = info["show"]
        if not show or self.is_generic_name(show):
            text = await self.ask_text(
                event,
                f"💬 «{orig_filename}» — субтитры к серии (сезон {info['season']}, серия {info['episode']}), "
                f"но название сериала я не нашёл.",
                "название сериала — например: Dune Prophecy",
            )
            show = self.clean_show_name(text or "")
            if not show or self.is_generic_name(show):
                if text:
                    await event.reply("🤷 Не разобрал название сериала — субтитры пропущены.")
                return False, None
        return True, {"show": show, "season": info["season"], "episode": info["episode"]}

    async def resolve_subtitle_target(self, url, user, password, job) -> tuple:
        """Match a subtitle to a remote video and update its filename and status location."""
        base, tags = self.split_sub_name(Path(job.filename).stem)
        ext = Path(job.filename).suffix
        directories = []
        video = None
        if job.series:
            season = int(job.series["season"])
            directories = await self.copyparty.resolve_series_dirs(
                url, user, password, job.series["show"], season
            )
            entries = await self.copyparty.list_remote_entries(url, user, password, directories)
            if entries and job.series.get("episode") is not None:
                video = self.pick_episode_video(entries[1], season, int(job.series["episode"]))
        else:
            entries = await self.copyparty.list_remote_entries(url, user, password, [])
            if entries:
                video = self.pick_movie_video(entries[1], base)

        if video:
            leaf = Path(video).stem + "".join("." + tag for tag in tags) + ext
            note = f"рядом с «{self.short_name(video, 50)}»"
        else:
            leaf = job.filename
            note = "видео не найдено — имя оставлено как есть"
        leaf = self.truncate_filename(leaf)
        job.filename = leaf
        where = "/".join(directories)
        where = f"{where} ({note})" if where else note
        return "/".join(directories + [leaf]), where
