"""Defaults for config."""
from typing import ClassVar


class ConfigDefaults:
    """Default configuration values for the Myrient Search App."""

    IGNORED_BASE_FOLDERS: tuple[str] = (
        #"who_lee",
        #"RetroAchievements",
        #"T-En Collection",
        #"Total DOS Collection",
        #"Internet Archive",
        #"No-Intro",
        #"TOSEC-ISO",
        #"TOSEC",
        #"Redump",
        "TOSEC-PIX",
        "Miscellaneous",
        "Touhou Project Collection",
        "bitsavers",
        "eXo",
        "TeknoParrot",
        "FinalBurn Neo",
        "HBMAME",
        "Hardware Target Game Database",
        "Lost Level",
        "MAME",
        "96x65pixels",
        "EBZero",
        "Unknown",
        "aberusugi",
        "aitus95",
        "archiver_2020",
        "bingbong294",
        "bluemaxima",
        "chadmaster",
        "cmpltromsets",
        "kodi_amp_spmc_canada",
        "lollo_220",
        "md_",
        "mdashk",
        "pixelspoil",
        "retro_game_champion",
        "romhacking_net",
        "rompacker",
        "rvt-r",
        "sketch_the_cow",
        "storage_manager",
        "superbio",
        "teamgt19",
        "the_last_collector",
        "yahweasel",
        )

    IGNORED_FOLDERS: tuple[str] = (
        "audio cd",
        "bd-video",
        "dvd-video",
        "video cd",
        "disc keys",
        "(themes)",
        "(updates)",
        "firmware",
        "demos",
        "docs",
        "gdi files",
        "applications",
        "operating systems",
        "various",
        "magaz",
        #'graphics',
        "educational",
        "samplers",
        "coverdiscs",
        "(music)",
        "diskmags",
        "books",
        "bios",
        "demo",
        "(movie only)",
        "documents",
        "video game osts",
        "video game scans",
        "source code",
        "playstation gameshark updates",
        "non-redump",
        "promo",
        "amiibo",
        "nintendo sdks",
        "ultimate codes",
        "action replay",
        "unlimited codes",
        "cheatcodes",
        "cheat code",
        "cheats",
        "cheat master",
        "cheat disc",
        )

    PLATFORM_ALIASES: ClassVar[dict[str, str]] = {
        # Apple
        "apple 1": "Apple I",
        "apple i": "Apple I",
        "apple ii": "Apple II",
        "apple ii plus": "Apple II Plus",
        "apple iie": "Apple IIe",
        "apple iigs": "Apple IIGS",

        # VM Labs
        "vm labs nuon": "VM Labs NUON",

        # NEC
        "nec pc engine cd & turbografx": "NEC PC Engine CD & TurboGrafx-16",
        "nec pc engine cd + turbografx": "NEC PC Engine CD & TurboGrafx-16",
        "nec pc-engine & turbografx-16": "NEC PC Engine & TurboGrafx-16",
        "nec pc-engine cd & turbografx-16": "NEC PC Engine CD & TurboGrafx-16",

        # SNK
        "snk neo geo": "SNK Neo Geo",
        "snk neo-geo": "SNK Neo Geo",
        "snk neo-geo cd": "SNK Neo Geo CD",
        "snk neogeo pocket": "SNK Neo Geo Pocket",
        "snk neogeo pocket color": "SNK Neo Geo Pocket Color",

        # Nintendo
        "nintendo famicom & entertainment system": "Nintendo Entertainment System",
        "nintendo super famicom & entertainment system":
            "Super Nintendo Entertainment System",
        "nintendo super nintendo entertainment system":
            "Super Nintendo Entertainment System",
        "nintendo super entertainment system": "Super Nintendo Entertainment System",
        "nintendo famicom disk system": "Nintendo Famicom Disk System",
        "nintendo wii [zstd-19-128k]": "Nintendo Wii",
        "nintendo gamecube [zstd-19-128k]": "Nintendo Gamecube",

        # IBM
        "ibm pc compatible": "IBM PC Compatible",
        "ibm pc and compatibles": "IBM PC Compatible",
        "ibm pc compatibles": "IBM PC Compatible",
        }
