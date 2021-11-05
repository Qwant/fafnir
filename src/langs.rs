use once_cell::sync::Lazy;
use std::collections::HashMap;

/// This map has been filled from https://en.wikipedia.org/wiki/ISO_3166-1
pub static COUNTRIES_LANGS: Lazy<HashMap<String, Vec<&'static str>>> = Lazy::new(|| {
    [
        ("AU", vec!["en"]),                   // australia
        ("AT", vec!["de"]),                   // austria
        ("BY", vec!["be", "ru"]),             // belarus
        ("BE", vec!["fr", "de", "nl"]),       // belgium
        ("BR", vec!["pt"]),                   // brazil
        ("BG", vec!["bg"]),                   // bulgaria
        ("CA", vec!["en", "fr"]),             // canada
        ("CN", vec!["zh"]),                   // china
        ("HR", vec!["hr"]),                   // croatia
        ("CZ", vec!["cs"]),                   // czechia
        ("DK", vec!["da"]),                   // denmark
        ("EE", vec!["et"]),                   // estonia
        ("FR", vec!["fr"]),                   // france
        ("DE", vec!["de"]),                   // germany
        ("GR", vec!["el"]),                   // greece
        ("IE", vec!["ga", "en"]),             // ireland
        ("IT", vec!["it"]),                   // italy
        ("JP", vec!["ja"]),                   // japan
        ("KR", vec!["ko"]),                   // south korea
        ("LV", vec!["lv"]),                   // latvia
        ("LT", vec!["lt"]),                   // lithuania
        ("LU", vec!["lb", "fr", "de"]),       // luxembourg
        ("MX", vec!["es"]),                   // mexico
        ("MD", vec!["ro"]),                   // moldova
        ("NL", vec!["nl"]),                   // netherlands
        ("NZ", vec!["en", "mi"]),             // new zealand
        ("MK", vec!["mk", "sq"]),             // north macedonia
        ("NO", vec!["no"]),                   // norway
        ("PL", vec!["pl"]),                   // poland
        ("PT", vec!["pt"]),                   // portugal
        ("RO", vec!["ro"]),                   // romania
        ("RU", vec!["ru"]),                   // russia
        ("RS", vec!["sr"]),                   // serbia
        ("SG", vec!["en", "ms", "ta"]),       // singapour
        ("SK", vec!["sk"]),                   // slovakia
        ("SL", vec!["sl"]),                   // slovenia
        ("ES", vec!["es"]),                   // spain
        ("SE", vec!["sv"]),                   // sweden
        ("CH", vec!["de", "fr", "it", "rm"]), // switzerland
        ("TH", vec!["th"]),                   // thailand
        ("TN", vec!["ar"]),                   // tunisia
        ("TR", vec!["tr"]),                   // turkey
        ("UA", vec!["uk"]),                   // ukraine
        ("GB", vec!["en"]),                   // united kingdom
        ("US", vec!["en"]),                   // usa
        ("UY", vec!["es"]),                   // uruguay
        ("UZ", vec!["uz"]),                   // uzbekistan
        ("VE", vec!["es"]),                   // venezuela
        ("VN", vec!["vi"]),                   // viet nam
    ]
    .into_iter()
    .map(|(key, val)| (key.to_string(), val))
    .collect()
});
