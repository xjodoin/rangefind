// Per-language stopword lists for the multilingual analysis profile.
//
// Lists are stored in natural orthography and folded through the analysis
// fold at module load, so membership tests always run against the same
// folded token forms the tokenizer emits. Lists stay deliberately short:
// high-frequency function words only, because stopword removal here is a
// posting-budget optimization, not a linguistic statement.

const RAW_STOPWORDS = {
  en: [
    "a", "an", "and", "are", "as", "at", "be", "been", "being", "but", "by",
    "can", "could", "did", "do", "does", "for", "from", "had", "has", "have",
    "he", "her", "his", "if", "in", "into", "is", "it", "its", "not", "of",
    "on", "or", "she", "than", "that", "the", "their", "then", "there",
    "these", "they", "this", "those", "to", "under", "was", "we", "were",
    "which", "will", "with", "within", "would", "you", "your"
  ],
  fr: [
    "au", "aux", "avec", "ce", "ces", "cette", "dans", "de", "des", "du",
    "elle", "en", "et", "eux", "il", "ils", "je", "la", "le", "les", "leur",
    "leurs", "lui", "ma", "mais", "me", "mes", "moi", "mon", "ne", "nos",
    "notre", "nous", "on", "ou", "où", "par", "pas", "pour", "qu", "que",
    "qui", "sa", "se", "ses", "son", "sur", "ta", "te", "tes", "toi", "ton",
    "tu", "un", "une", "vos", "votre", "vous", "y"
  ],
  de: [
    "aber", "als", "am", "an", "auch", "auf", "aus", "bei", "bin", "bis",
    "das", "dass", "dem", "den", "der", "des", "die", "durch", "ein", "eine",
    "einem", "einen", "einer", "eines", "er", "es", "für", "hat", "hatte",
    "ich", "ihr", "im", "in", "ist", "mit", "nach", "nicht", "noch", "nur",
    "oder", "sein", "sich", "sie", "sind", "so", "über", "um", "und", "vom",
    "von", "vor", "war", "was", "wie", "wird", "zu", "zum", "zur"
  ],
  es: [
    "a", "al", "algo", "como", "con", "de", "del", "el", "ella", "ellas",
    "ellos", "en", "entre", "era", "es", "esta", "este", "esto", "fue", "ha",
    "han", "hay", "la", "las", "le", "les", "lo", "los", "más", "me", "mi",
    "muy", "no", "nos", "o", "para", "pero", "por", "que", "se", "ser",
    "si", "sin", "sobre", "son", "su", "sus", "también", "te", "tiene",
    "un", "una", "uno", "y", "ya", "yo"
  ],
  it: [
    "a", "ad", "ai", "al", "alla", "alle", "anche", "che", "chi", "ci",
    "come", "con", "da", "dai", "dal", "dalla", "degli", "dei", "del",
    "della", "delle", "dello", "di", "e", "ed", "era", "gli", "ha", "hanno",
    "i", "il", "in", "la", "le", "lo", "loro", "ma", "mi", "ne", "nel",
    "nella", "non", "o", "per", "più", "quella", "quello", "questa",
    "questo", "se", "si", "sono", "su", "sua", "sul", "sulla", "suo", "un",
    "una", "uno", "è"
  ],
  pt: [
    "a", "ao", "aos", "as", "às", "com", "como", "da", "das", "de", "dela",
    "dele", "do", "dos", "e", "é", "ela", "ele", "em", "entre", "era",
    "foi", "for", "isso", "isto", "já", "mais", "mas", "me", "mesmo", "na",
    "não", "nas", "no", "nos", "o", "os", "ou", "para", "pela", "pelo",
    "por", "que", "se", "sem", "ser", "seu", "sua", "são", "também", "tem",
    "um", "uma", "você"
  ],
  nl: [
    "aan", "als", "bij", "dan", "dat", "de", "der", "des", "deze", "die",
    "dit", "door", "een", "en", "er", "haar", "heeft", "het", "hij", "hun",
    "ik", "in", "is", "je", "kan", "maar", "met", "naar", "niet", "nog",
    "of", "om", "onder", "ook", "op", "over", "te", "tot", "uit", "van",
    "voor", "was", "wat", "wordt", "zijn", "zo"
  ],
  sv: [
    "att", "av", "den", "det", "de", "dem", "der", "du", "efter", "ej",
    "eller", "en", "ett", "för", "från", "han", "hans", "har", "hon", "i",
    "inte", "jag", "kan", "man", "med", "men", "mot", "när", "och", "om",
    "på", "samma", "sig", "sin", "sitt", "som", "till", "under", "upp",
    "var", "vad", "vid", "vi", "än", "är", "över"
  ],
  no: [
    "at", "av", "da", "de", "den", "der", "det", "din", "du", "eller",
    "en", "er", "et", "etter", "for", "fra", "ha", "han", "hans", "har",
    "hun", "hva", "hvor", "i", "ikke", "jeg", "kan", "man", "med", "men",
    "mot", "når", "og", "om", "opp", "over", "på", "seg", "sin", "sitt",
    "skal", "som", "til", "under", "var", "ved", "vi", "vil", "være", "å"
  ],
  da: [
    "af", "alle", "at", "da", "de", "den", "der", "det", "dette", "dig",
    "din", "du", "efter", "eller", "en", "er", "et", "for", "fra", "han",
    "hans", "har", "havde", "hun", "hvad", "hvor", "i", "ikke", "jeg",
    "kan", "man", "med", "men", "mod", "når", "og", "om", "op", "over",
    "på", "sig", "sin", "skal", "som", "til", "under", "var", "ved", "vi",
    "vil", "være", "år"
  ],
  fi: [
    "että", "ei", "he", "hän", "ja", "jo", "jos", "kanssa", "kuin", "kun",
    "me", "mikä", "mitä", "mutta", "myös", "ne", "niin", "nyt", "olen",
    "oli", "olla", "on", "ovat", "se", "sen", "siitä", "sinä", "tai",
    "tämä", "te", "vain", "voi", "vielä"
  ],
  ru: [
    "а", "бы", "был", "в", "вы", "да", "для", "до", "его", "ее", "если",
    "есть", "же", "за", "и", "из", "или", "их", "к", "как", "ко", "когда",
    "кто", "мы", "на", "не", "него", "нее", "ни", "но", "о", "об", "он",
    "она", "они", "оно", "от", "по", "под", "при", "с", "со", "так",
    "также", "то", "того", "только", "том", "ты", "у", "уже", "что",
    "чтобы", "эта", "эти", "это", "этот", "я"
  ],
  el: [
    "αλλά", "από", "για", "δεν", "είναι", "εν", "ένα", "έναν", "ενός",
    "επί", "η", "ή", "θα", "και", "κατά", "με", "μια", "να", "ο", "οι",
    "όπως", "ότι", "ου", "παρά", "που", "προς", "πως", "σε", "στη",
    "στην", "στο", "στον", "τα", "την", "της", "τι", "τις", "το", "τον",
    "του", "των", "ως"
  ],
  ar: [
    "ال", "إلى", "الى", "أن", "ان", "أو", "او", "إن", "التي", "الذي",
    "بعد", "بين", "ثم", "حتى", "على", "عن", "عند", "غير", "في", "قبل",
    "قد", "كان", "كانت", "كل", "كما", "لا", "لم", "لن", "له", "لها",
    "ما", "مع", "من", "منذ", "هذا", "هذه", "هو", "هي", "و", "ولا",
    "وما", "ومن", "يكون"
  ],
  hi: [
    "अगर", "और", "इन", "इस", "उन", "उस", "एक", "एवं", "कर", "करना", "का",
    "कि", "किया", "की", "के", "को", "गया", "जब", "जो", "तक", "तथा", "तो",
    "था", "थी", "थे", "नहीं", "ने", "पर", "फिर", "बाद", "भी", "मैं", "में",
    "यह", "या", "ये", "वह", "वे", "से", "ही", "है", "हैं", "हो", "होता"
  ],
  tr: [
    "acaba", "ama", "ancak", "bir", "bu", "da", "daha", "de", "değil",
    "gibi", "hem", "hep", "her", "ile", "ise", "için", "kadar", "ki",
    "mi", "mı", "mu", "mü", "ne", "o", "olan", "olarak", "sonra", "şu",
    "ve", "veya", "ya", "yani"
  ],
  pl: [
    "aby", "ale", "był", "była", "było", "być", "co", "czy", "dla", "do",
    "go", "i", "ich", "jak", "jako", "je", "jego", "jej", "jest", "już",
    "lub", "ma", "na", "nie", "o", "od", "oraz", "po", "pod", "przez",
    "się", "są", "ta", "tak", "także", "te", "tego", "tej", "ten", "to",
    "tym", "w", "we", "z", "za", "ze", "że"
  ],
  cs: [
    "a", "aby", "ale", "ani", "bude", "byl", "byla", "bylo", "být", "co",
    "což", "do", "i", "jak", "jako", "je", "jeho", "její", "jen", "ještě",
    "jsem", "jsou", "k", "kde", "když", "která", "které", "který", "ma",
    "mezi", "na", "nebo", "není", "o", "od", "po", "pod", "pro", "před",
    "s", "se", "si", "tak", "také", "tato", "ten", "to", "tohoto", "toto",
    "u", "v", "ve", "však", "z", "za", "že"
  ],
  id: [
    "ada", "adalah", "akan", "atau", "bahwa", "dan", "dari", "dengan",
    "di", "dia", "ia", "ini", "itu", "juga", "kami", "karena", "ke",
    "kita", "lebih", "oleh", "pada", "para", "saya", "sebagai", "sudah",
    "telah", "tidak", "untuk", "yang"
  ],
  hu: [
    "a", "az", "azt", "be", "csak", "de", "egy", "el", "ez", "ezt", "fel",
    "hogy", "is", "ki", "le", "lesz", "meg", "mint", "mit", "nem", "pedig",
    "s", "és", "vagy", "van", "volt"
  ],
  ro: [
    "a", "acest", "această", "al", "ale", "care", "ce", "cu", "de", "din",
    "după", "el", "ea", "este", "fi", "fost", "în", "între", "la", "le",
    "lor", "lui", "mai", "nu", "o", "pe", "pentru", "prin", "sa", "sau",
    "se", "și", "sunt", "un", "una", "unei", "unui"
  ]
};

// Folding happens in analysis.js; this module stays data-only to avoid an
// import cycle, so it exports the raw lists and analysis.js folds them once.
export function rawStopwordLists() {
  return RAW_STOPWORDS;
}

export const STOPWORD_LANGUAGES = Object.freeze(Object.keys(RAW_STOPWORDS));
