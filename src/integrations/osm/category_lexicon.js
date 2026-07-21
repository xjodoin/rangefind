// Category lexicon for OSM query intents.
//
// The old planner carried a hand-picked list of seven category words; every
// uncovered one ("cinema", "bakery", "bar", …) fell through to the locality
// resolver, and OSM contains villages named Cinema — so a bare category query
// could teleport the map across the planet. The vocabulary was never ours to
// curate: the index already knows every `type` facet value the corpus holds
// (amenity=cinema, shop=bakery, …). This module turns that vocabulary into
// the category gate.
//
// Three sources, in order of authority:
//   1. `category_lexicon` embedded in a sharded root manifest at build time —
//      the corpus's own type vocabulary joined with the alias table below.
//   2. A single index's lazy `type` facet dictionary (one small cached read).
//   3. The bundled canonical vocabulary — common OSM type values — so
//      indexes built before this artifact existed still resolve categories.
//
// Aliases are code, vocabulary is data: the translations below are stable
// OSM-tag domain knowledge shipped with the package, while the set of type
// values that actually exist comes from each index.

export const OSM_CATEGORY_LEXICON_VERSION = 1;

// Common OSM type values (amenity/shop/leisure/tourism/historic/healthcare
// keys) — the bundled fallback vocabulary for indexes without an embedded
// lexicon. Deliberately omits values that collide with famous place names
// more often than they are queried as categories ("spa" — Spa, Belgium);
// an embedded lexicon built from a corpus that holds the type restores them.
export const OSM_CANONICAL_TYPES = Object.freeze([
  // food & drink
  "restaurant", "cafe", "fast_food", "bar", "pub", "biergarten",
  "food_court", "ice_cream", "nightclub",
  // health
  "pharmacy", "hospital", "clinic", "doctors", "dentist", "veterinary",
  "optician", "chemist",
  // money & services
  "bank", "atm", "bureau_de_change", "post_office", "police",
  "fire_station", "townhall", "courthouse", "embassy", "library",
  "community_centre", "childcare", "kindergarten", "school", "college",
  "university", "driving_school", "place_of_worship", "grave_yard",
  "marketplace", "toilets", "recycling",
  // transport
  "fuel", "charging_station", "parking", "bicycle_parking", "car_wash",
  "car_rental", "bicycle_rental", "bus_station", "ferry_terminal", "taxi",
  // shops — food
  "supermarket", "convenience", "bakery", "butcher", "greengrocer",
  "seafood", "deli", "cheese", "chocolate", "confectionery", "pastry",
  "alcohol", "beverages", "coffee", "tea", "health_food", "farm",
  // shops — goods
  "clothes", "shoes", "jewelry", "boutique", "department_store", "mall",
  "second_hand", "charity", "antiques", "books", "stationery", "gift",
  "toys", "sports", "outdoor", "bicycle", "car", "car_repair", "car_parts",
  "motorcycle", "tyres", "hardware", "doityourself", "paint", "electronics",
  "computer", "mobile_phone", "appliance", "furniture", "kitchen", "bed",
  "carpet", "florist", "garden_centre", "pet", "music",
  "musical_instrument", "photo", "video_games", "newsagent", "kiosk",
  "tobacco", "variety_store", "general",
  // shops — services
  "hairdresser", "beauty", "cosmetics", "massage", "tattoo", "laundry",
  "dry_cleaning", "travel_agency", "copyshop", "funeral_directors",
  "tailor", "storage_rental",
  // leisure
  "park", "playground", "garden", "nature_reserve", "sports_centre",
  "fitness_centre", "swimming_pool", "water_park", "stadium", "pitch",
  "golf_course", "miniature_golf", "ice_rink", "bowling_alley",
  "escape_game", "amusement_arcade", "dog_park", "marina", "sauna", "beach",
  // tourism & historic
  "hotel", "motel", "hostel", "guest_house", "apartment", "chalet",
  "camp_site", "caravan_site", "museum", "gallery", "attraction",
  "viewpoint", "information", "theme_park", "zoo", "aquarium", "cinema",
  "theatre", "casino", "castle", "monument", "memorial", "ruins",
  "archaeological_site"
]);

// type → alternate surfaces users actually type: French forms (the demo's
// second language), English synonyms, and irregular plurals the generic
// singular-stripping in lookupCategory cannot derive. Regular plurals
// ("cinemas", "boulangeries") never need listing.
export const OSM_TYPE_ALIASES = Object.freeze({
  restaurant: ["resto", "restos"],
  cafe: ["café", "coffee", "coffee shop"],
  fast_food: ["restauration rapide"],
  bar: [],
  pub: ["brasserie"],
  ice_cream: ["crème glacée", "glacier"],
  nightclub: ["boîte de nuit", "night club"],
  pharmacy: ["pharmacie", "drugstore", "drug store"],
  hospital: ["hôpital", "hôpitaux"],
  clinic: ["clinique"],
  doctors: ["doctor", "médecin", "médecins", "docteur"],
  dentist: ["dentiste"],
  veterinary: ["vétérinaire", "vet"],
  optician: ["opticien", "lunetterie"],
  bank: ["banque"],
  atm: ["guichet automatique", "cash machine", "distributeur"],
  post_office: ["bureau de poste", "poste"],
  fire_station: ["caserne de pompiers", "pompiers"],
  townhall: ["hôtel de ville", "mairie", "city hall", "town hall"],
  courthouse: ["palais de justice", "tribunal"],
  embassy: ["ambassade"],
  library: ["bibliothèque"],
  community_centre: ["centre communautaire", "community center"],
  childcare: ["garderie", "daycare"],
  kindergarten: ["maternelle"],
  school: ["école"],
  university: ["université"],
  place_of_worship: ["church", "église", "mosque", "mosquée", "synagogue",
    "temple"],
  grave_yard: ["cemetery", "cimetière"],
  marketplace: ["marché", "farmers market", "marché public"],
  toilets: ["toilettes", "washroom", "restroom", "wc"],
  fuel: ["essence", "station essence", "station-service", "gas station",
    "petrol station"],
  charging_station: ["borne de recharge", "ev charging"],
  parking: ["stationnement"],
  car_wash: ["lave-auto"],
  bus_station: ["gare routière", "bus terminal"],
  ferry_terminal: ["traversier"],
  supermarket: ["supermarché", "épicerie", "grocery", "grocery store"],
  convenience: ["dépanneur", "convenience store", "corner store"],
  bakery: ["boulangerie"],
  butcher: ["boucherie"],
  greengrocer: ["fruiterie"],
  seafood: ["poissonnerie"],
  deli: ["charcuterie"],
  cheese: ["fromagerie"],
  chocolate: ["chocolaterie", "chocolatier"],
  confectionery: ["confiserie"],
  pastry: ["pâtisserie"],
  alcohol: ["liquor store", "liquor"],
  health_food: ["produits naturels", "health food store"],
  clothes: ["vêtements", "clothing", "clothing store"],
  shoes: ["chaussures", "shoe store"],
  jewelry: ["bijouterie", "jewellery"],
  department_store: ["grand magasin"],
  mall: ["centre commercial", "shopping mall", "shopping centre",
    "shopping center"],
  books: ["librairie", "bookstore", "book store", "bookshop"],
  toys: ["jouets", "toy store"],
  bicycle: ["vélo", "bike shop", "bike store"],
  car: ["car dealer", "car dealership", "concessionnaire"],
  car_repair: ["garage", "mécanicien", "auto repair"],
  hardware: ["quincaillerie"],
  doityourself: ["bricolage", "home improvement"],
  electronics: ["électronique"],
  furniture: ["meubles"],
  florist: ["fleuriste", "flower shop"],
  garden_centre: ["jardinerie", "garden center"],
  pet: ["animalerie", "pet store", "pet shop"],
  tobacco: ["tabagie", "tabac"],
  hairdresser: ["coiffeur", "salon de coiffure", "barber", "barbier"],
  beauty: ["beauty salon", "salon de beauté"],
  laundry: ["buanderie", "laverie", "laundromat"],
  dry_cleaning: ["nettoyeur", "pressing"],
  travel_agency: ["agence de voyage"],
  park: ["parc"],
  playground: ["aire de jeux"],
  garden: ["jardin"],
  sports_centre: ["centre sportif", "sports center"],
  fitness_centre: ["gym", "fitness", "salle de sport", "fitness center"],
  swimming_pool: ["piscine", "pool"],
  stadium: ["stade"],
  golf_course: ["golf", "terrain de golf"],
  ice_rink: ["patinoire", "skating rink"],
  bowling_alley: ["bowling", "quilles"],
  dog_park: ["parc à chiens"],
  hotel: ["hôtel"],
  hostel: ["auberge"],
  guest_house: ["gîte"],
  camp_site: ["camping", "campground"],
  museum: ["musée"],
  gallery: ["galerie"],
  viewpoint: ["point de vue", "belvédère"],
  information: ["tourist information", "office de tourisme"],
  theme_park: ["parc d'attractions", "amusement park"],
  cinema: ["cinéma", "movie theater", "movie theatre", "movies"],
  theatre: ["théâtre", "theater"],
  castle: ["château"],
  police: ["poste de police", "police station"]
});

// Types that must never gate as categories. Place and address values are
// how localities are typed — if "city" became a category word, "Quebec
// City" would turn into a nearest-city search around Québec instead of
// resolving the city itself.
export const CATEGORY_EXCLUDED_TYPES = new Set([
  "address", "interpolated_address_range", "postal_code",
  "city", "town", "village", "hamlet", "municipality", "borough",
  "suburb", "neighbourhood", "quarter", "locality", "isolated_dwelling",
  "island", "islet", "archipelago", "region", "district", "province",
  "state", "county", "country", "continent", "sea", "ocean"
]);

// The planner's shared text folding: accent-strip, lowercase, and collapse
// punctuation to spaces so "Crème-Glacée" and "creme glacee" meet.
export function fold(value) {
  return String(value || "")
    .normalize("NFKD")
    .replaceAll(/\p{M}+/gu, "")
    .toLocaleLowerCase("en-US")
    .replaceAll(/[^\p{L}\p{N}]+/gu, " ")
    .trim();
}

// The searchable surface of a type value — mirrors the extraction's
// typeLabel so lexicon text queries hit the label baked into every doc body
// ("fast_food" → "fast food").
export function typeQueryText(type) {
  return String(type || "").replaceAll(/[_;]+/gu, " ").trim();
}

function labelize(folded) {
  return folded.replaceAll(/\b\p{L}/gu, letter => letter.toLocaleUpperCase("en-US"));
}

function addEntry(lexicon, key, type) {
  const folded = fold(key);
  if (!folded || lexicon.has(folded)) return;
  lexicon.set(folded, { type, query: typeQueryText(type) });
}

// OSM `type` is a freeform tag value: a planet corpus holds tens of
// thousands of one-off strings ("church tent", "school mother touch") that
// would make terrible category gates. A gate must look like a category
// word: short, wordy, and not a place/address value.
function gateableType(type) {
  if (CATEGORY_EXCLUDED_TYPES.has(type)) return false;
  const folded = fold(typeQueryText(type));
  return Boolean(folded)
    && folded.length <= 32
    && folded.split(" ").length <= 3
    && /\p{L}/u.test(folded);
}

// Build the folded alias → {type, query} map. `typeValues` may be:
//   - null/empty → bundled canonical vocabulary
//   - an array of strings or facet {value} rows (a type facet dictionary)
//   - an embedded artifact: { types: [...], aliases: { folded: type } }
export function buildCategoryLexicon(typeValues = null) {
  // Aliases outrank raw type identities: "coffee" is a real OSM type
  // (shop=coffee, bean roasters), but a person typing "coffee" wants cafés —
  // the curated alias encodes intent, the identity only encodes existence.
  const lexicon = new Map();
  if (typeValues && !Array.isArray(typeValues) && Array.isArray(typeValues.types)) {
    for (const [alias, type] of Object.entries(typeValues.aliases || {})) {
      addEntry(lexicon, alias, type);
    }
    for (const type of typeValues.types) addEntry(lexicon, typeQueryText(type), type);
    return lexicon;
  }
  const types = Array.isArray(typeValues) && typeValues.length
    ? typeValues.map(item => String(item?.value ?? item ?? "")).filter(gateableType)
    : [...OSM_CANONICAL_TYPES];
  const present = new Set(types);
  for (const [type, aliases] of Object.entries(OSM_TYPE_ALIASES)) {
    if (!present.has(type)) continue;
    for (const alias of aliases) addEntry(lexicon, alias, type);
  }
  for (const type of types) addEntry(lexicon, typeQueryText(type), type);
  return lexicon;
}

// Compose the artifact a sharded root manifest embeds: the corpus's own type
// vocabulary joined with the alias table, resolved once at build time so
// query runtimes need no package-version agreement to translate.
//
// `minCount` prunes the freeform tail: a planet corpus carries ~37k distinct
// type values of which ~1.2k appear 250+ times — the rest are one-off
// strings that would bloat the root manifest and misgate real queries.
// Curated types (canonical vocabulary or alias table) always survive, so
// small corpora and demo fixtures keep their categories.
export function buildCategoryLexiconArtifact(typeValues, { minCount = 250 } = {}) {
  const curated = new Set([...OSM_CANONICAL_TYPES, ...Object.keys(OSM_TYPE_ALIASES)]);
  const types = (typeValues || [])
    .map(item => ({ value: String(item?.value ?? item ?? ""), n: Number(item?.n || 0) }))
    .filter(item => item.value
      && gateableType(item.value)
      && (curated.has(item.value) || item.n >= minCount));
  const present = new Set(types.map(item => item.value));
  const aliases = {};
  for (const [type, list] of Object.entries(OSM_TYPE_ALIASES)) {
    if (!present.has(type)) continue;
    for (const alias of list) {
      const folded = fold(alias);
      if (folded && !(folded in aliases)) aliases[folded] = type;
    }
  }
  return {
    version: OSM_CATEGORY_LEXICON_VERSION,
    facet: "type",
    types: types.map(item => item.value),
    aliases
  };
}

// Singular candidates for a folded surface: exact form first, then the
// regular plural strips ("cinemas" → "cinema", "churches" → "church",
// "chevaux"/"hôpitaux" → "cheval"/"hopital", "parcs" → "parc").
function lookupKeys(folded) {
  const keys = [folded];
  if (folded.endsWith("aux")) keys.push(`${folded.slice(0, -3)}al`);
  if (folded.endsWith("es")) keys.push(folded.slice(0, -2));
  if (folded.endsWith("s") || folded.endsWith("x")) keys.push(folded.slice(0, -1));
  return keys;
}

// Match a folded surface against the lexicon. Returns the intent the planner
// consumes: `query` is the canonical searchable text, `label` echoes the
// user's own words back in the receipt ("Boulangeries nearby").
export function lookupCategory(lexicon, surface) {
  const folded = fold(surface);
  if (!folded) return null;
  for (const key of lookupKeys(folded)) {
    const entry = lexicon.get(key);
    if (entry) return { query: entry.query, label: labelize(folded) };
  }
  return null;
}

let bundledLexicon = null;

// The zero-configuration lexicon: canonical vocabulary + aliases, used when
// an engine carries no embedded artifact and no reachable facet dictionary.
export function defaultCategoryLexicon() {
  if (!bundledLexicon) bundledLexicon = buildCategoryLexicon(null);
  return bundledLexicon;
}
