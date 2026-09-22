//! Always-on server tool: Chinese lunar calendar & festival lookup.

use serde_json::{Value, json};

use crate::protocol::ToolDef;
use lunar_lite::{LunarDate, SolarDate, lunar_to_solar, solar_to_lunar};

pub const CHINESE_CALENDAR_NAME: &str = "chinese_calendar";

pub fn chinese_calendar_tool_def() -> ToolDef {
    ToolDef {
        name: CHINESE_CALENDAR_NAME.into(),
        description: "查询公历/农历日期互转，以及常见中国节日（含农历节日如中秋、春节）对应的公历日期。\
             用户问节假日日期、农历几月几日、某公历对应农历时必须调用本工具，不要凭记忆猜测。"
            .into(),
        parameters: json!({
            "type": "object",
            "properties": {
                "action": {
                    "type": "string",
                    "enum": ["festival", "lunar_to_solar", "solar_to_lunar"],
                    "description": "festival=按节日名查公历；lunar_to_solar=农历转公历；solar_to_lunar=公历转农历"
                },
                "festival": {
                    "type": "string",
                    "description": "节日名，如 中秋、春节、端午、元宵、重阳、七夕、腊八、除夕、元旦、清明、劳动节、国庆。action=festival 时必填"
                },
                "year": {
                    "type": "integer",
                    "description": "公历年份。festival / lunar_to_solar 时使用（农历年通常与该公历年同号）"
                },
                "month": {
                    "type": "integer",
                    "description": "月（1-12）。lunar_to_solar / solar_to_lunar 时必填"
                },
                "day": {
                    "type": "integer",
                    "description": "日。lunar_to_solar / solar_to_lunar 时必填"
                },
                "leap_month": {
                    "type": "boolean",
                    "description": "是否闰月；仅 lunar_to_solar 可选，默认 false"
                }
            },
            "required": ["action"]
        }),
        readonly: Some(true),
    }
}

pub fn apply_chinese_calendar(args: &Value) -> Result<String, String> {
    let action = args
        .get("action")
        .and_then(|v| v.as_str())
        .ok_or_else(|| "missing action".to_string())?;

    match action {
        "festival" => {
            let name = args
                .get("festival")
                .and_then(|v| v.as_str())
                .map(str::trim)
                .filter(|s| !s.is_empty())
                .ok_or_else(|| "festival requires festival name".to_string())?;
            let year = int_field(args, "year")
                .ok_or_else(|| "festival requires year (Gregorian)".to_string())?;
            lookup_festival(name, year)
        }
        "lunar_to_solar" => {
            let year = int_field(args, "year").ok_or_else(|| "missing year".to_string())?;
            let month = int_field(args, "month").ok_or_else(|| "missing month".to_string())? as u8;
            let day = int_field(args, "day").ok_or_else(|| "missing day".to_string())? as u8;
            let leap = args
                .get("leap_month")
                .and_then(|v| v.as_bool())
                .unwrap_or(false);
            let solar = lunar_to_solar(LunarDate {
                year,
                month,
                day,
                is_leap_month: leap,
            })
            .map_err(|e| format!("lunar_to_solar failed: {e}"))?;
            Ok(format!(
                "农历{}年{}{}月{}日 → 公历{}年{}月{}日",
                year,
                if leap { "闰" } else { "" },
                month,
                day,
                solar.year,
                solar.month,
                solar.day
            ))
        }
        "solar_to_lunar" => {
            let year = int_field(args, "year").ok_or_else(|| "missing year".to_string())?;
            let month = int_field(args, "month").ok_or_else(|| "missing month".to_string())? as u8;
            let day = int_field(args, "day").ok_or_else(|| "missing day".to_string())? as u8;
            let lunar = solar_to_lunar(SolarDate { year, month, day })
                .map_err(|e| format!("solar_to_lunar failed: {e}"))?;
            Ok(format!(
                "公历{}年{}月{}日 → 农历{}年{}{}月{}日",
                year,
                month,
                day,
                lunar.year,
                if lunar.is_leap_month { "闰" } else { "" },
                lunar.month,
                lunar.day
            ))
        }
        other => Err(format!("unknown action: {other}")),
    }
}

fn int_field(args: &Value, key: &str) -> Option<i32> {
    args.get(key).and_then(|v| {
        v.as_i64()
            .map(|n| n as i32)
            .or_else(|| v.as_u64().map(|n| n as i32))
            .or_else(|| v.as_f64().map(|n| n as i32))
    })
}

fn lookup_festival(name: &str, year: i32) -> Result<String, String> {
    let key = normalize_festival(name);
    // Fixed solar festivals
    let solar_fixed: Option<(u8, u8)> = match key.as_str() {
        "元旦" => Some((1, 1)),
        "劳动节" | "五一" => Some((5, 1)),
        "国庆" | "国庆节" => Some((10, 1)),
        _ => None,
    };
    if let Some((m, d)) = solar_fixed {
        return Ok(format!(
            "{year}年「{name}」为公历{year}年{m}月{d}日（固定公历节日）"
        ));
    }

    // Lunar festivals: (lunar month, lunar day). Mid-Autumn = 八月十五.
    let lunar: Option<(u8, u8, &str)> = match key.as_str() {
        "春节" | "过年" | "农历新年" => Some((1, 1, "正月初一")),
        "元宵" | "元宵节" | "灯节" => Some((1, 15, "正月十五")),
        "端午" | "端午节" | "端阳" => Some((5, 5, "五月初五")),
        "七夕" | "七夕节" => Some((7, 7, "七月初七")),
        "中秋" | "中秋节" => Some((8, 15, "八月十五")),
        "重阳" | "重阳节" => Some((9, 9, "九月初九")),
        "腊八" | "腊八节" => Some((12, 8, "腊月初八")),
        _ => None,
    };

    if let Some((lm, ld, lunar_label)) = lunar {
        let solar = lunar_to_solar(LunarDate {
            year,
            month: lm,
            day: ld,
            is_leap_month: false,
        })
        .map_err(|e| format!("festival lunar convert failed: {e}"))?;
        return Ok(format!(
            "{year}年「{name}」为公历{}年{}月{}日（农历{lunar_label}）",
            solar.year, solar.month, solar.day
        ));
    }

    // 除夕 = day before 春节 of (year+1) lunar... actually 除夕 is last day of lunar year,
    // which is the day before 正月初一 of the *next* lunar year. For "2026年除夕",
    // users usually mean the eve before Spring Festival that falls in early 2026 or late 2025.
    // Common: year Y 除夕 = day before lunar new year of year Y (i.e. before 正月初一 of lunar year Y).
    if matches!(key.as_str(), "除夕") {
        let spring = lunar_to_solar(LunarDate {
            year,
            month: 1,
            day: 1,
            is_leap_month: false,
        })
        .map_err(|e| format!("festival lunar convert failed: {e}"))?;
        let eve = pred_solar(spring)?;
        return Ok(format!(
            "{year}年「除夕」为公历{}年{}月{}日（春节前一天）",
            eve.year, eve.month, eve.day
        ));
    }

    // 清明 — approximate via solar term would need more API; give a short refusal with hint
    if matches!(key.as_str(), "清明" | "清明节") {
        return Err(
            "清明为二十四节气，请改用公历约 4月4–6日查询，或换用其他已支持节日".into(),
        );
    }

    Err(format!(
        "unsupported festival: {name}. 支持：春节/元宵/端午/七夕/中秋/重阳/腊八/除夕/元旦/劳动节/国庆"
    ))
}

fn normalize_festival(name: &str) -> String {
    name.chars()
        .filter(|c| !c.is_whitespace())
        .collect::<String>()
        .replace("节", "")
        .replace("節日", "")
}

fn pred_solar(d: SolarDate) -> Result<SolarDate, String> {
    use chrono::Datelike;
    let date = chrono::NaiveDate::from_ymd_opt(d.year, d.month as u32, d.day as u32)
        .ok_or_else(|| format!("invalid solar date {}-{}-{}", d.year, d.month, d.day))?;
    let prev = date
        .pred_opt()
        .ok_or_else(|| "cannot compute previous day".to_string())?;
    Ok(SolarDate {
        year: prev.year(),
        month: prev.month() as u8,
        day: prev.day() as u8,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn mid_autumn_2026() {
        let out = apply_chinese_calendar(&json!({
            "action": "festival",
            "festival": "中秋节",
            "year": 2026
        }))
        .unwrap();
        assert!(
            out.contains("2026年9月25日"),
            "got: {out}"
        );
    }

    #[test]
    fn lunar_roundtrip_sample() {
        let out = apply_chinese_calendar(&json!({
            "action": "lunar_to_solar",
            "year": 2026,
            "month": 8,
            "day": 15
        }))
        .unwrap();
        assert!(out.contains("公历"), "{out}");
        assert!(out.contains("9月25日"), "{out}");
    }
}
