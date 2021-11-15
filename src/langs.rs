use once_cell::sync::Lazy;
use std::collections::HashMap;

/// This map has been filled from https://en.wikipedia.org/wiki/ISO_3166-1
pub static COUNTRIES_LANGS: Lazy<HashMap<&str, &[&str]>> = Lazy::new(|| {
    [
        ("AU", &["en"] as &[_]),           // australia
        ("AT", &["de"]),                   // austria
        ("BY", &["be", "ru"]),             // belarus
        ("BE", &["fr", "de", "nl"]),       // belgium
        ("BR", &["pt"]),                   // brazil
        ("BG", &["bg"]),                   // bulgaria
        ("CA", &["en", "fr"]),             // canada
        ("CN", &["zh"]),                   // china
        ("HR", &["hr"]),                   // croatia
        ("CZ", &["cs"]),                   // czechia
        ("DK", &["da"]),                   // denmark
        ("EE", &["et"]),                   // estonia
        ("FR", &["fr"]),                   // france
        ("DE", &["de"]),                   // germany
        ("GR", &["el"]),                   // greece
        ("IE", &["ga", "en"]),             // ireland
        ("IT", &["it"]),                   // italy
        ("JP", &["ja"]),                   // japan
        ("KR", &["ko"]),                   // south korea
        ("LV", &["lv"]),                   // latvia
        ("LT", &["lt"]),                   // lithuania
        ("LU", &["lb", "fr", "de"]),       // luxembourg
        ("MX", &["es"]),                   // mexico
        ("MD", &["ro"]),                   // moldova
        ("NL", &["nl"]),                   // netherlands
        ("NZ", &["en", "mi"]),             // new zealand
        ("MK", &["mk", "sq"]),             // north macedonia
        ("NO", &["no"]),                   // norway
        ("PL", &["pl"]),                   // poland
        ("PT", &["pt"]),                   // portugal
        ("RO", &["ro"]),                   // romania
        ("RU", &["ru"]),                   // russia
        ("RS", &["sr"]),                   // serbia
        ("SG", &["en", "ms", "ta"]),       // singapour
        ("SK", &["sk"]),                   // slovakia
        ("SL", &["sl"]),                   // slovenia
        ("ES", &["es"]),                   // spain
        ("SE", &["sv"]),                   // sweden
        ("CH", &["de", "fr", "it", "rm"]), // switzerland
        ("TH", &["th"]),                   // thailand
        ("TN", &["ar"]),                   // tunisia
        ("TR", &["tr"]),                   // turkey
        ("UA", &["uk"]),                   // ukraine
        ("GB", &["en"]),                   // united kingdom
        ("US", &["en"]),                   // usa
        ("UY", &["es"]),                   // uruguay
        ("UZ", &["uz"]),                   // uzbekistan
        ("VE", &["es"]),                   // venezuela
        ("VN", &["vi"]),                   // viet nam
    ]
    .into_iter()
    .collect()
});
