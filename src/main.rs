mod database;

use std::{env, ops::Range};

use chrono::{DateTime, Datelike, Days, NaiveDate, TimeDelta, Timelike, Utc, Weekday};
use chrono_tz::Tz;
use database::Database;
use itertools::Itertools;
use rand::{Rng, SeedableRng, rngs::StdRng};

use crate::database::CsvRecord;

const DATABASE_PATH: &str = "/home/gluon/var/irc/bots/Vettel/data/";
const DATABASE_COLLECTION: &str = "first_results";
const MAX_RESULTS: usize = 10;
const CUTOFF_US: i64 = 1000000;
const RAND_OPEN_HOUR: Range<u32> = 5..12;
const RAND_OPEN_MIN: Range<u32> = 0..59;

#[derive(Debug, PartialEq)]
struct FirstResult {
    nick: String,
    channel: String,
    datetime: DateTime<Utc>,
    timezone: String,
}

impl CsvRecord for FirstResult {
    fn from_fields(fields: &[String]) -> Self {
        Self {
            nick: fields[0].clone(),
            channel: fields[1].clone(),
            datetime: fields[2].clone().parse().unwrap_or_default(),
            timezone: fields[3].clone(),
        }
    }

    fn to_fields(&self) -> Vec<String> {
        vec![
            self.nick.clone(),
            self.channel.clone(),
            self.datetime.to_string(),
            self.timezone.clone(),
        ]
    }
}

pub enum Period {
    Day,
    Daily,
    Week,
    Weekly,
    Month,
    Monthly,
    Year,
    Yearly,
    Unknown,
}

fn main() -> Result<(), &'static str> {
    let mut args = env::args();
    let channel = match args.nth(2) {
        Some(channel) => channel,
        None => {
            println!("A channel must be provided");
            return Err("A channel must be provided");
        }
    };
    let span: DateTime<Utc> = match args.next() {
        Some(span) => match span.as_str() {
            "daily" => start_date(Period::Daily),
            "day" | "today" => start_date(Period::Day),
            "week" => start_date(Period::Week),
            "weekly" => start_date(Period::Weekly),
            "month" => start_date(Period::Month),
            "monthly" => start_date(Period::Monthly),
            "year" => start_date(Period::Year),
            "yearly" => start_date(Period::Yearly),
            _ => start_date(Period::Unknown),
        },
        None => DateTime::default(),
    };

    let db = Database::new(DATABASE_PATH, None);

    let first_results = match db.select(DATABASE_COLLECTION, |r: &FirstResult| {
        r.channel.to_lowercase() == channel.to_lowercase() && r.datetime >= span
    }) {
        Ok(Some(results)) => results,
        _ => {
            println!("No results found");
            return Err("No results found");
        }
    };

    let rank = rank(&first_results, MAX_RESULTS)?;

    println!("Top !first results (smallest gaps to the opening time of winners):");

    for (pos, (date, x)) in rank.iter().enumerate() {
        println!(
            "{}. {:?} {} {} ms",
            pos + 1,
            date,
            x.get(0).ok_or("Could not get data")?.1,
            x.get(0).ok_or("Could not get data")?.0 / 1000
        );
    }

    Ok(())
}

/// Compute the start date based on the period of time we want to go back in time.
fn start_date(period: Period) -> DateTime<Utc> {
    let now = Utc::now();
    let days = match period {
        Period::Daily => 1,
        Period::Day => {
            return now
                .checked_sub_signed(TimeDelta::hours(now.hour() as i64))
                .unwrap_or_default();
        }
        Period::Month => now.day(),
        Period::Monthly => 30,
        Period::Week => match now.weekday() {
            Weekday::Mon => 1,
            Weekday::Tue => 2,
            Weekday::Wed => 3,
            Weekday::Thu => 4,
            Weekday::Fri => 5,
            Weekday::Sat => 6,
            Weekday::Sun => 7,
        },
        Period::Weekly => 7,
        Period::Year => now
            .signed_duration_since(
                DateTime::parse_from_str(
                    format!("{}-12-31 11:59 +0000", now.year() - 1).as_str(),
                    "%Y-%m-%d %H:%M %z",
                )
                .unwrap_or_default(),
            )
            .num_days() as u32,
        Period::Yearly => 365,
        Period::Unknown => return DateTime::default(),
    };

    now.checked_sub_days(Days::new(days as u64))
        .unwrap_or_default()
}

/// Compute the top MAX_RESULTS earliest !1st submissions for each nick.
///
/// 1. Group entries by date (each different day of the year is a key for the group).
/// 2. For each date:
///    - Compute each player's "delta" (how close they were to the opening time).
///    - Keep only deltas of interest (positive and below cutoff).
///    - Pick the earliest valid one for that day.
/// 3. Globally sort all days by delta time (earliest !1st).
/// 4. Ensure unique entries by nick.
/// 5. Return the top MAX_RESULTS.
fn rank(
    first_results: &[FirstResult],
    max_results: usize,
) -> Result<Vec<(NaiveDate, Vec<(i64, String)>)>, &'static str> {
    // Group entries by date (each different day of the year is a key for the group).
    // Chain date_naive() to get rid of the time and return a date as key to chunk_by.
    let groups = first_results.iter().chunk_by(|r| {
        let tz: Tz = r
            .timezone
            .parse()
            .expect("Timezone should be in Continent/Capital format");

        r.datetime.with_timezone(&tz).date_naive()
    });

    // For each group (one per date), determine the best player and time delta.
    // The outer filter_map itereates through each date and selects where the best delta is between 0 and CUTOFF_US.
    // Then sorts the groups by the lowest delta, makes results unique by nick and takes max_results.
    let rank: Vec<(NaiveDate, Vec<(i64, String)>)> = groups
        .into_iter()
        .filter_map(|(day, group)| {
            // The inner filter_map calculates for each date the deltas, sorts by lowest and takes only one.
            // filter_map maps to Vec<(i64, String)>, a vector of tuples representing delta and nick.
            let delta_results: Vec<(i64, String)> = group
                .filter_map(|r| delta(day, r).ok())
                .sorted_by(|a, b| Ord::cmp(&a.0, &b.0))
                .take(1)
                .collect();
            // End of inner filter_map.

            if let Some((micros, _nick)) = delta_results.get(0) {
                if *micros > 0 && *micros <= CUTOFF_US {
                    return Some((day, delta_results));
                }
            }
            None
        })
        .sorted_by(|a, b| Ord::cmp(&a.1[0].0, &b.1[0].0))
        .unique_by(|r| r.1[0].1.clone())
        .take(max_results)
        .collect();
    // End of outer filter_map.

    Ok(rank)
}

/// Calculate the delta in microseconds between the time when the user played !1st and the opening time.
fn delta(day: NaiveDate, r: &FirstResult) -> Result<(i64, String), &'static str> {
    // Convert the player time to the player timezone.
    let tz: Tz = r.timezone.parse().map_err(|_| "Bad timezone")?;
    let local_player_time = r.datetime.with_timezone(&tz);

    let month_day = day.day();

    // Use the same seed as the bot uses (day of the month) to get the same opening hour.
    let mut rng = StdRng::seed_from_u64(month_day as u64);
    let open_hour = rng.random_range(RAND_OPEN_HOUR);

    // Use the same seed as the bot uses (day of the month) to get the same opening minute.
    let mut rng = StdRng::seed_from_u64(month_day as u64);
    let open_min = rng.random_range(RAND_OPEN_MIN);

    // To build the local opening time we use a little trick.
    // We already calculated the opening hour and minute above, but we are working with DateTime.
    // So we make the local opening time equal to the local player time to get the correct date.
    // Then we simply set the opening hour and minute with the values above.
    // Finally we zero out the other components of the DateTime.
    let local_opening_time = local_player_time
        .with_hour(open_hour)
        .and_then(|t| t.with_minute(open_min))
        .and_then(|t| t.with_second(0))
        .and_then(|t| t.with_nanosecond(0))
        .ok_or("Bad time format")?;

    // Finally subtract the local opening time from the local player time.
    let delta = local_player_time - local_opening_time;

    Ok((
        delta
            .num_microseconds()
            .ok_or("Could not get microseconds")?,
        r.nick.clone(),
    ))
}
