use std::{collections::HashMap, sync::OnceLock};

use crate::parser::Node;

pub fn rename_nodes(nodes: &mut [Node]) {
    let mut country_totals: HashMap<String, usize> = HashMap::new();

    for node in nodes.iter() {
        let base_name = country_label(&node.raw_name);
        *country_totals.entry(base_name).or_insert(0) += 1;
    }

    let mut country_seen: HashMap<String, usize> = HashMap::new();
    for node in nodes.iter_mut() {
        let base_name = country_label(&node.raw_name);
        let total = country_totals.get(&base_name).copied().unwrap_or(1);
        let index = country_seen.entry(base_name.clone()).or_insert(0);
        *index += 1;
        node.display_name = if total > 1 {
            format!("{base_name} #{}", *index)
        } else {
            base_name
        };
    }
}

fn country_label(raw_name: &str) -> String {
    if let Some((flag, code)) = extract_flag(raw_name) {
        let country = country_map()
            .get(code.as_str())
            .copied()
            .unwrap_or("Unknown");
        return format!("{flag} {country}");
    }

    "🌐 Unknown".to_string()
}

fn extract_flag(input: &str) -> Option<(String, String)> {
    let chars: Vec<char> = input.chars().collect();
    for window in chars.windows(2) {
        let first = *window.first()?;
        let second = *window.get(1)?;
        if is_regional_indicator(first) && is_regional_indicator(second) {
            let code = format!(
                "{}{}",
                regional_indicator_to_ascii(first)?,
                regional_indicator_to_ascii(second)?
            );
            return Some((format!("{first}{second}"), code));
        }
    }
    None
}

fn is_regional_indicator(character: char) -> bool {
    matches!(character as u32, 0x1F1E6..=0x1F1FF)
}

fn regional_indicator_to_ascii(character: char) -> Option<char> {
    let offset = (character as u32).checked_sub(0x1F1E6)?;
    char::from_u32(u32::from(b'A') + offset)
}

fn country_map() -> &'static HashMap<&'static str, &'static str> {
    static COUNTRY_MAP: OnceLock<HashMap<&'static str, &'static str>> = OnceLock::new();
    COUNTRY_MAP.get_or_init(|| {
        HashMap::from([
            ("AC", "Ascension Island"),
            ("AD", "Andorra"),
            ("AE", "United Arab Emirates"),
            ("AF", "Afghanistan"),
            ("AG", "Antigua and Barbuda"),
            ("AI", "Anguilla"),
            ("AL", "Albania"),
            ("AM", "Armenia"),
            ("AO", "Angola"),
            ("AQ", "Antarctica"),
            ("AR", "Argentina"),
            ("AS", "American Samoa"),
            ("AT", "Austria"),
            ("AU", "Australia"),
            ("AW", "Aruba"),
            ("AX", "Aland Islands"),
            ("AZ", "Azerbaijan"),
            ("BA", "Bosnia and Herzegovina"),
            ("BB", "Barbados"),
            ("BD", "Bangladesh"),
            ("BE", "Belgium"),
            ("BF", "Burkina Faso"),
            ("BG", "Bulgaria"),
            ("BH", "Bahrain"),
            ("BI", "Burundi"),
            ("BJ", "Benin"),
            ("BL", "Saint Barthelemy"),
            ("BM", "Bermuda"),
            ("BN", "Brunei"),
            ("BO", "Bolivia"),
            ("BQ", "Caribbean Netherlands"),
            ("BR", "Brazil"),
            ("BS", "Bahamas"),
            ("BT", "Bhutan"),
            ("BV", "Bouvet Island"),
            ("BW", "Botswana"),
            ("BY", "Belarus"),
            ("BZ", "Belize"),
            ("CA", "Canada"),
            ("CC", "Cocos Islands"),
            ("CD", "Democratic Republic of the Congo"),
            ("CF", "Central African Republic"),
            ("CG", "Republic of the Congo"),
            ("CH", "Switzerland"),
            ("CI", "Cote d'Ivoire"),
            ("CK", "Cook Islands"),
            ("CL", "Chile"),
            ("CM", "Cameroon"),
            ("CN", "China"),
            ("CO", "Colombia"),
            ("CP", "Clipperton Island"),
            ("CR", "Costa Rica"),
            ("CU", "Cuba"),
            ("CV", "Cape Verde"),
            ("CW", "Curacao"),
            ("CX", "Christmas Island"),
            ("CY", "Cyprus"),
            ("CZ", "Czechia"),
            ("DE", "Germany"),
            ("DG", "Diego Garcia"),
            ("DJ", "Djibouti"),
            ("DK", "Denmark"),
            ("DM", "Dominica"),
            ("DO", "Dominican Republic"),
            ("DZ", "Algeria"),
            ("EA", "Ceuta and Melilla"),
            ("EC", "Ecuador"),
            ("EE", "Estonia"),
            ("EG", "Egypt"),
            ("EH", "Western Sahara"),
            ("ER", "Eritrea"),
            ("ES", "Spain"),
            ("ET", "Ethiopia"),
            ("EU", "European Union"),
            ("FI", "Finland"),
            ("FJ", "Fiji"),
            ("FK", "Falkland Islands"),
            ("FM", "Micronesia"),
            ("FO", "Faroe Islands"),
            ("FR", "France"),
            ("GA", "Gabon"),
            ("GB", "United Kingdom"),
            ("GD", "Grenada"),
            ("GE", "Georgia"),
            ("GF", "French Guiana"),
            ("GG", "Guernsey"),
            ("GH", "Ghana"),
            ("GI", "Gibraltar"),
            ("GL", "Greenland"),
            ("GM", "Gambia"),
            ("GN", "Guinea"),
            ("GP", "Guadeloupe"),
            ("GQ", "Equatorial Guinea"),
            ("GR", "Greece"),
            ("GS", "South Georgia and the South Sandwich Islands"),
            ("GT", "Guatemala"),
            ("GU", "Guam"),
            ("GW", "Guinea-Bissau"),
            ("GY", "Guyana"),
            ("HK", "Hong Kong"),
            ("HM", "Heard Island and McDonald Islands"),
            ("HN", "Honduras"),
            ("HR", "Croatia"),
            ("HT", "Haiti"),
            ("HU", "Hungary"),
            ("IC", "Canary Islands"),
            ("ID", "Indonesia"),
            ("IE", "Ireland"),
            ("IL", "Israel"),
            ("IM", "Isle of Man"),
            ("IN", "India"),
            ("IO", "British Indian Ocean Territory"),
            ("IQ", "Iraq"),
            ("IR", "Iran"),
            ("IS", "Iceland"),
            ("IT", "Italy"),
            ("JE", "Jersey"),
            ("JM", "Jamaica"),
            ("JO", "Jordan"),
            ("JP", "Japan"),
            ("KE", "Kenya"),
            ("KG", "Kyrgyzstan"),
            ("KH", "Cambodia"),
            ("KI", "Kiribati"),
            ("KM", "Comoros"),
            ("KN", "Saint Kitts and Nevis"),
            ("KP", "North Korea"),
            ("KR", "South Korea"),
            ("KW", "Kuwait"),
            ("KY", "Cayman Islands"),
            ("KZ", "Kazakhstan"),
            ("LA", "Laos"),
            ("LB", "Lebanon"),
            ("LC", "Saint Lucia"),
            ("LI", "Liechtenstein"),
            ("LK", "Sri Lanka"),
            ("LR", "Liberia"),
            ("LS", "Lesotho"),
            ("LT", "Lithuania"),
            ("LU", "Luxembourg"),
            ("LV", "Latvia"),
            ("LY", "Libya"),
            ("MA", "Morocco"),
            ("MC", "Monaco"),
            ("MD", "Moldova"),
            ("ME", "Montenegro"),
            ("MF", "Saint Martin"),
            ("MG", "Madagascar"),
            ("MH", "Marshall Islands"),
            ("MK", "North Macedonia"),
            ("ML", "Mali"),
            ("MM", "Myanmar"),
            ("MN", "Mongolia"),
            ("MO", "Macau"),
            ("MP", "Northern Mariana Islands"),
            ("MQ", "Martinique"),
            ("MR", "Mauritania"),
            ("MS", "Montserrat"),
            ("MT", "Malta"),
            ("MU", "Mauritius"),
            ("MV", "Maldives"),
            ("MW", "Malawi"),
            ("MX", "Mexico"),
            ("MY", "Malaysia"),
            ("MZ", "Mozambique"),
            ("NA", "Namibia"),
            ("NC", "New Caledonia"),
            ("NE", "Niger"),
            ("NF", "Norfolk Island"),
            ("NG", "Nigeria"),
            ("NI", "Nicaragua"),
            ("NL", "Netherlands"),
            ("NO", "Norway"),
            ("NP", "Nepal"),
            ("NR", "Nauru"),
            ("NU", "Niue"),
            ("NZ", "New Zealand"),
            ("OM", "Oman"),
            ("PA", "Panama"),
            ("PE", "Peru"),
            ("PF", "French Polynesia"),
            ("PG", "Papua New Guinea"),
            ("PH", "Philippines"),
            ("PK", "Pakistan"),
            ("PL", "Poland"),
            ("PM", "Saint Pierre and Miquelon"),
            ("PN", "Pitcairn Islands"),
            ("PR", "Puerto Rico"),
            ("PS", "Palestine"),
            ("PT", "Portugal"),
            ("PW", "Palau"),
            ("PY", "Paraguay"),
            ("QA", "Qatar"),
            ("RE", "Reunion"),
            ("RO", "Romania"),
            ("RS", "Serbia"),
            ("RU", "Russia"),
            ("RW", "Rwanda"),
            ("SA", "Saudi Arabia"),
            ("SB", "Solomon Islands"),
            ("SC", "Seychelles"),
            ("SD", "Sudan"),
            ("SE", "Sweden"),
            ("SG", "Singapore"),
            ("SH", "Saint Helena"),
            ("SI", "Slovenia"),
            ("SJ", "Svalbard and Jan Mayen"),
            ("SK", "Slovakia"),
            ("SL", "Sierra Leone"),
            ("SM", "San Marino"),
            ("SN", "Senegal"),
            ("SO", "Somalia"),
            ("SR", "Suriname"),
            ("SS", "South Sudan"),
            ("ST", "Sao Tome and Principe"),
            ("SV", "El Salvador"),
            ("SX", "Sint Maarten"),
            ("SY", "Syria"),
            ("SZ", "Eswatini"),
            ("TA", "Tristan da Cunha"),
            ("TC", "Turks and Caicos Islands"),
            ("TD", "Chad"),
            ("TF", "French Southern Territories"),
            ("TG", "Togo"),
            ("TH", "Thailand"),
            ("TJ", "Tajikistan"),
            ("TK", "Tokelau"),
            ("TL", "Timor-Leste"),
            ("TM", "Turkmenistan"),
            ("TN", "Tunisia"),
            ("TO", "Tonga"),
            ("TR", "Turkey"),
            ("TT", "Trinidad and Tobago"),
            ("TV", "Tuvalu"),
            ("TW", "Taiwan"),
            ("TZ", "Tanzania"),
            ("UA", "Ukraine"),
            ("UG", "Uganda"),
            ("UM", "United States Minor Outlying Islands"),
            ("US", "United States"),
            ("UY", "Uruguay"),
            ("UZ", "Uzbekistan"),
            ("VA", "Vatican City"),
            ("VC", "Saint Vincent and the Grenadines"),
            ("VE", "Venezuela"),
            ("VG", "British Virgin Islands"),
            ("VI", "U.S. Virgin Islands"),
            ("VN", "Vietnam"),
            ("VU", "Vanuatu"),
            ("WF", "Wallis and Futuna"),
            ("WS", "Samoa"),
            ("XK", "Kosovo"),
            ("YE", "Yemen"),
            ("YT", "Mayotte"),
            ("ZA", "South Africa"),
            ("ZM", "Zambia"),
            ("ZW", "Zimbabwe"),
        ])
    })
}
