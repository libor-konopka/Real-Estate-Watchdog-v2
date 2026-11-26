import re

# ==========================================
# INFRASTRUCTURE AND NETWORKS
# ==========================================
RE_WATER_PUBLIC = re.compile(r"vodovod|městská\s+voda|obecní\s+voda", re.IGNORECASE)
RE_WATER_WELL = re.compile(r"studna|studnu", re.IGNORECASE)
RE_WATER_BOREHOLE = re.compile(r"\bvrt\b|vrtan[áé]\s+studn[auy]", re.IGNORECASE)

RE_WASTE_SEWER = re.compile(r"kanalizac|obecní\s+odpad|stočn[eé]", re.IGNORECASE)
RE_WASTE_SEPTIC = re.compile(r"septik", re.IGNORECASE)
RE_WASTE_CESSPOOL = re.compile(r"jímk[auy]|žump[auy]", re.IGNORECASE)

RE_ELEC = re.compile(
    r"\belekt\w+|\bel\.|230\s*V|400\s*V|220\s*V|380\s*V", re.IGNORECASE
)
RE_NO_ELEC = re.compile(
    r"bez\s+(?:zavedené\s+)?elektřiny|bez\s+el\.|elektřina\s+(?:zde\s+)?není",
    re.IGNORECASE,
)
RE_GAS = re.compile(r"\bplyn(?:u|em|y|ov[ýáéím]+)?\b", re.IGNORECASE)
RE_NO_GAS = re.compile(
    r"bez\s+(?:zavedeného\s+)?plynu|plyn\s+(?:zde\s+)?není",
    re.IGNORECASE,
)
RE_ALL_NETWORKS = re.compile(
    r"vešker[eé]\s+(?:inženýrsk[eé]\s+)?sítě|"
    r"všechny\s+(?:inženýrsk[eé]\s+)?sítě|"
    r"inženýrsk[eéých]+\s+sít[ěíchm]+",
    re.IGNORECASE,
)

RE_HEAT_GAS = re.compile(
    r"(?:plynov|kondenzačn)[ýím]+\s+kot[el][im]?|wawky?", re.IGNORECASE
)
RE_HEAT_PUMP = re.compile(r"tepeln[éým]+\s+čerpadl[oem]?", re.IGNORECASE)
RE_HEAT_ELEC = re.compile(
    r"elektrick[ýým]+\s+kot[el][im]?|přímotop[y]?|el\.\s*kotel", re.IGNORECASE
)
RE_HEAT_SOLID = re.compile(
    r"tuh[áéý]\w*\s+paliv\w*|krbov[áéý]\w*\s+kamn\w*|pelet\w*|\bkrb(?:u|em|y)?\b",
    re.IGNORECASE,
)
RE_HEAT_CENTRAL = re.compile(
    r"ústřední|dálkov[eéým]+\s+vytápění|teplárn", re.IGNORECASE
)

# ==========================================
# LEGAL CONTEXT AND AVAILABILITY
# ==========================================
RE_OWN_PERSONAL = re.compile(r"osobní[mho]?\s+vlastnictví|\bOV\b", re.IGNORECASE)
RE_OWN_COOP = re.compile(r"družstevní[mho]?\s+vlastnictví|\bDV\b", re.IGNORECASE)
RE_OWN_MUNI = re.compile(
    r"(?:obecní(?:m|ho)?|městsk[éý](?:m|ho)?)\s+(?:vlastnictví|byt\w*|dům|domu)|"
    r"vlastnictví\s+(?:obce|města)",
    re.IGNORECASE,
)

RE_FORECLOSURE = re.compile(r"exekuc[ei]", re.IGNORECASE)
RE_INSOLVENCY = re.compile(r"insolvenc[ei]", re.IGNORECASE)
RE_SHARE = re.compile(
    r"spoluvlastnick[ýéýmho]+\s+podíl|"
    r"podíl\s+(?:o\s+velikosti|1/|2/|3/|4/|5/|6/|7/|8/|9/)",
    re.IGNORECASE,
)

RE_PRICE_NOTE = re.compile(
    r"((?:včetně|bez|\+)[^\.\n]{0,15}?"
    r"(?:provize|dph|poplatků|právního\s+servisu)[^\.\n]*)",
    re.IGNORECASE,
)
RE_AVAILABLE_FWD = re.compile(
    r"(?:voln[ýáé]|k\s+nastěhování|k\s+dispozici)\s+(?:od\s+)?"
    r"(ihned|\d{1,2}\.\s*\d{1,2}\.\s*\d{4})",
    re.IGNORECASE,
)
RE_AVAILABLE_REV = re.compile(
    r"(ihned|\d{1,2}\.\s*\d{1,2}\.\s*\d{4})\s+"
    r"(?:k\s+dispozici|k\s+nastěhování|voln[ýáé])",
    re.IGNORECASE,
)

# ==========================================
# AREA EXTRACTION (Land)
# ==========================================
RE_LAND_AREA_FWD = re.compile(
    r"(?:pozem\w+|zahrad\w+|parcel\w+|dv[ůo]r\w*)\D{0,30}?"
    r"(\d+(?:[ \xa0]\d+)*(?:[.,]\d+)?)\s*(?:m[2²\'’´`]?|metr\w*)(?=[.,\s]|$)",
    re.IGNORECASE,
)
RE_LAND_AREA_REV = re.compile(
    r"(\d+(?:[ \xa0]\d+)*(?:[.,]\d+)?)\s*(?:m[2²\'’´`]?|metr\w*)(?=[.,\s]|$)"
    r"\D{0,30}?(?:pozem\w+|zahrad\w+|parcel\w+|dv[ůo]r\w*)",
    re.IGNORECASE,
)
RE_LAND_AREA_STRUCT = re.compile(
    r"(?:plocha\s+(?:parcely|pozemku|zahrady|dvora)|pozemek|dvůr)\D{0,20}?:\s*"
    r"(\d+(?:[ \xa0]\d+)*(?:[.,]\d+)?)",
    re.IGNORECASE,
)

# ==========================================
# AREA EXTRACTION (Usable / Floor)
# ==========================================
RE_USABLE_AREA_FWD = re.compile(
    r"(?:užitn\w+|podlahov\w+|celkov\w+)\s+"
    r"(?:plo(?:ch|š)\w+|výměr\w+|rozloh\w+|velikos\w+)\D{0,40}?"
    r"(\d+(?:[ \xa0]\d+)*(?:[.,]\d+)?)\s*"
    r"(?:m[2²\'’´`]?|metr\w*|m\s*čtverečn\w*)(?=[.,\s/)]|$)",
    re.IGNORECASE,
)

RE_USABLE_AREA_REV = re.compile(
    r"(\d+(?:[ \xa0]\d+)*(?:[.,]\d+)?)\s*"
    r"(?:m[2²\'’´`]?|metr\w*|m\s*čtverečn\w*)(?=[.,\s/)]|$)\D{0,40}?"
    r"(?:užitn\w+|podlahov\w+|celkov\w+)\s+"
    r"(?:plo(?:ch|š)\w+|výměr\w+|rozloh\w+|velikos\w+)",
    re.IGNORECASE,
)

RE_USABLE_AREA_STRUCT = re.compile(
    r"(?:užitná|podlahová|celková)\s+"
    r"(?:plocha|výměra|rozloha|velikost)[^:]{0,20}?:\s*"
    r"(\d+(?:[ \xa0]\d+)*(?:[.,]\d+)?)",
    re.IGNORECASE,
)

# ==========================================
# AREA EXTRACTION (Built-up)
# ==========================================
RE_BUILT_UP_AREA_FWD = re.compile(
    r"zastavěn\w+\s+ploch\w+\D{0,30}?"
    r"(\d+(?:[ \xa0]\d+)*(?:[.,]\d+)?)\s*(?:m[2²\'’´`]?|metr\w*)(?=[.,\s]|$)",
    re.IGNORECASE,
)
RE_BUILT_UP_AREA_REV = re.compile(
    r"(\d+(?:[ \xa0]\d+)*(?:[.,]\d+)?)\s*(?:m[2²\'’´`]?|metr\w*)(?=[.,\s]|$)"
    r"\D{0,30}?zastavěn\w+\s+ploch\w+",
    re.IGNORECASE,
)
RE_BUILT_UP_AREA_STRUCT = re.compile(
    r"zastavěná\s+plocha\D{0,20}?:\s*(\d+(?:[ \xa0]\d+)*(?:[.,]\d+)?)",
    re.IGNORECASE,
)

# ==========================================
# BUILDING MATERIAL
# ==========================================
RE_MAT_BRICK = re.compile(r"\bcihl\w+", re.IGNORECASE)
RE_MAT_PANEL = re.compile(r"\bpanel\w+", re.IGNORECASE)
RE_MAT_WOOD = re.compile(
    r"\bdřevostavb\w+|\bdřevěn\w+|\bsrub\w*|\broubenk\w+", re.IGNORECASE
)
RE_MAT_MIXED = re.compile(r"\bsmíšen\w+", re.IGNORECASE)

# ==========================================
# BUILDING CONDITION
# ==========================================
RE_COND_NEW = re.compile(
    r"\bnovostavb\w*|stav\s+(?:objektu|bytu)[^:]{0,15}?:\s*novostavba",
    re.IGNORECASE,
)
RE_COND_RENOVATED = re.compile(
    r"\b(?:po\s+(?:kompletní\s+|celkové\s+|částečné\s+)?rekonstrukci|"
    r"zrekonstruovan\w+)\b|stav\s+objektu[^:]{0,15}?:\s*po\s+rekonstrukci",
    re.IGNORECASE,
)
RE_COND_BEFORE_RECON = re.compile(
    r"\b(?:před\s+rekonstrukcí|k\s+rekonstrukci|původní\s+stav)\b|"
    r"stav\s+objektu[^:]{0,15}?:\s*před\s+rekonstrukcí",
    re.IGNORECASE,
)
RE_COND_GOOD = re.compile(
    r"\b(?:dobrý\s+stav|udržovan\w+)\b|stav\s+objektu[^:]{0,15}?:\s*dobrý",
    re.IGNORECASE,
)
RE_COND_CONSTRUCTION = re.compile(
    r"\b(?:ve\s+výstavbě|hrub[áé]\s+stavb[ae]|před\s+dokončením)\b|"
    r"stav\s+objektu[^:]{0,15}?:\s*ve\s+výstavbě",
    re.IGNORECASE,
)

# ==========================================
# ENERGY CLASS (PENB)
# ==========================================
RE_ENERGY_CLASS = re.compile(
    r"(?i:(?:PENB|energetick[aáé]\s+(?:tříd[ay]|náročnost\w*)|"
    r"třída\s+energetické\s+náročnosti))"
    r"(?:[\s:\-]+|(?i:je|třída|třídy|budovy|skupina)){0,4}\s*"
    r"\b([A-Ga-g])\b"
)

# ==========================================
# LOCATION (ZIP code and municipality)
# ==========================================
RE_LOCATION = re.compile(r"(?:(?:PSČ|PSC)?\s*(\d{3}\s?\d{2}))?\s*(.*)", re.IGNORECASE)
