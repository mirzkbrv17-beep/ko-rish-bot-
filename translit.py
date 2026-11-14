"""Ingliz (lotin) alifbosidagi matnni rus alifbosiga o'girish."""

MULTI = {
    "shch": "щ", "sch": "щ",
    "sh": "ш", "ch": "ч", "zh": "ж", "kh": "х", "ts": "ц",
    "yo": "ё", "yu": "ю", "ya": "я", "ye": "е", "ph": "ф", "th": "т",
    "ck": "к", "ee": "и", "oo": "у",
}
SINGLE = {
    "a": "а", "b": "б", "c": "к", "d": "д", "e": "е", "f": "ф", "g": "г",
    "h": "х", "i": "и", "j": "дж", "k": "к", "l": "л", "m": "м", "n": "н",
    "o": "о", "p": "п", "q": "к", "r": "р", "s": "с", "t": "т", "u": "у",
    "v": "в", "w": "в", "x": "х", "y": "й", "z": "з",
}
APOS = "'`’‘ʻʼ"
VOWELS = "aeiouy"


def _case(src: str, out: str) -> str:
    if len(src) > 1 and src.isupper():
        return out.upper()
    if src[0].isupper():
        return out[0].upper() + out[1:]
    return out


def to_russian(text: str) -> str:
    res, i, n = [], 0, len(text)
    while i < n:
        ch = text[i]
        low = ch.lower()
        if low in APOS:
            i += 1
            continue
        matched = False
        for size in (4, 3, 2):
            chunk = text[i:i + size]
            if len(chunk) == size and chunk.lower() in MULTI:
                res.append(_case(chunk, MULTI[chunk.lower()]))
                i += size
                matched = True
                break
        if matched:
            continue
        if low in SINGLE:
            out = SINGLE[low]
            prev = text[i - 1].lower() if i > 0 else " "
            if low == "e" and not prev.isalpha():
                out = "э"  # so'z boshidagi E -> Э
            if low == "c" and i + 1 < n and text[i + 1].lower() in "eiy":
                out = "ц"
            res.append(_case(ch, out))
        else:
            res.append(ch)
        i += 1
    return "".join(res)
