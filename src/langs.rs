use once_cell::sync::Lazy;
use std::collections::HashMap;

// This map has been filled from https://en.wikipedia.org/wiki/ISO_3166-1
pub static COUNTRIES_LANGS: Lazy<HashMap<String, Vec<&'static str>>> = Lazy::new(|| {
    [
        // australia
        ("AU", vec!["en"]),
        // austria
        ("AT", vec!["de"]),
        // belarus
        ("BY", vec!["be", "ru"]),
        // belgium
        ("BE", vec!["fr", "de", "nl"]),
        // brazil
        ("BR", vec!["pt"]),
        // bulgaria
        ("BG", vec!["bg"]),
        // canada
        ("CA", vec!["en", "fr"]),
        // china
        ("CN", vec!["zh"]),
        // croatia
        ("HR", vec!["hr"]),
        // czechia
        ("CZ", vec!["cs"]),
        // denmark
        ("DK", vec!["da"]),
        // estonia
        ("EE", vec!["et"]),
        // france
        ("FR", vec!["fr"]),
        // germany
        ("DE", vec!["de"]),
        // greece
        ("GR", vec!["el"]),
        // ireland
        ("IE", vec!["ga", "en"]),
        // italy
        ("IT", vec!["it"]),
        // japan
        ("JP", vec!["ja"]),
        // south korea
        ("KR", vec!["ko"]),
        // latvia
        ("LV", vec!["lv"]),
        // lithuania
        ("LT", vec!["lt"]),
        // luxembourg
        ("LU", vec!["lb", "fr", "de"]),
        // mexico
        ("MX", vec!["es"]),
        // moldova
        ("MD", vec!["ro"]),
        // netherlands
        ("NL", vec!["nl"]),
        // new zealand
        ("NZ", vec!["en", "mi"]),
        // north macedonia
        ("MK", vec!["mk", "sq"]),
        // norway
        ("NO", vec!["no"]),
        // poland
        ("PL", vec!["pl"]),
        // portugal
        ("PT", vec!["pt"]),
        // romania
        ("RO", vec!["ro"]),
        // russia
        ("RU", vec!["ru"]),
        // serbia
        ("RS", vec!["sr"]),
        // singapour
        ("SG", vec!["en", "ms", "ta"]),
        // slovakia
        ("SK", vec!["sk"]),
        // slovenia
        ("SL", vec!["sl"]),
        // spain
        ("ES", vec!["es"]),
        // sweden
        ("SE", vec!["sv"]),
        // switzerland
        ("CH", vec!["de", "fr", "it", "rm"]),
        // thailand
        ("TH", vec!["th"]),
        // tunisia
        ("TN", vec!["ar"]),
        // turkey
        ("TR", vec!["tr"]),
        // ukraine
        ("UA", vec!["uk"]),
        // united kingdom
        ("GB", vec!["en"]),
        // usa
        ("US", vec!["en"]),
        // uruguay
        ("UY", vec!["es"]),
        // uzbekistan
        ("UZ", vec!["uz"]),
        // venezuela
        ("VE", vec!["es"]),
        // viet nam
        ("VN", vec!["vi"]),
    ]
    .iter()
    .map(|(a, b)| (a.to_string(), b.clone()))
    .collect()
});
