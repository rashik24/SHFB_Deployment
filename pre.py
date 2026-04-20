import pandas as pd
import numpy as np
from itertools import product
from datetime import timedelta
import json
import re

# =========================================================================
# 📂 FILE PATHS
# =========================================================================
SHFB_SUPPLY_FILE = "SHFB Data.xlsx"
SHFB_GEO_FILE    = "SHFB geo.csv"
ODM_FILE         = "ODM 3 SHFB.xlsx"
GEO_INFO_FILE    = "Geo Data SHFB.csv"
GEO_MAP_FILE     = "GeoID RUCA.csv"
SHFB_OUTPUT_FILE = "shfb_3b.csv"

# =========================================================================
# 🔧 HELPER FUNCTIONS
# =========================================================================
def clean_address(addr: str) -> str:
    if pd.isna(addr):
        return addr
    addr = addr.lower().strip().replace(".", "")
    replacements = {
        r"\bst\b": "street", r"\brd\b": "road", r"\bave\b": "avenue",
        r"\bblvd\b": "boulevard", r"\bln\b": "lane", r"\bdr\b": "drive",
        r"\bct\b": "court", r"\bhwy\b": "highway", r"\bpkwy\b": "parkway",
        r"\bn\b": "north", r"\bs\b": "south", r"\be\b": "east", r"\bw\b": "west"
    }
    for pattern, repl in replacements.items():
        addr = re.sub(pattern, repl, addr)
    return re.sub(r"\s+", " ", addr).strip()


def parse_time_str(tstr: str):
    if not isinstance(tstr, str):
        raise ValueError("not a string")
    s = tstr.strip().lower()
    if s in {"noon", "12 noon"}:
        s = "12:00 pm"
    elif s in {"midnight", "12 midnight"}:
        s = "12:00 am"
    for fmt in ("%I:%M %p", "%I %p"):
        try:
            ts = pd.to_datetime(s, format=fmt)
            return ts.replace(year=1900, month=1, day=1)
        except Exception:
            continue
    ts = pd.to_datetime(s, errors="raise")
    return ts.replace(year=1900, month=1, day=1)


def expand_to_hour_bins(open_ts, close_ts):
    if close_ts <= open_ts:
        close_ts += timedelta(days=1)
    hours = []
    current = open_ts.floor("H")
    while current < close_ts:
        hours.append(current.hour)
        current += timedelta(hours=1)
    return hours


def normalize_week_list(week_field):
    if week_field is None or week_field == "":
        return []
    if isinstance(week_field, str):
        tokens = re.split(r"[^\d]+", week_field)
        week_field = [t for t in tokens if t]
    normalized = []
    for w in (week_field if isinstance(week_field, (list, tuple)) else [week_field]):
        digits = re.sub(r"\D", "", str(w))
        if digits:
            normalized.append(int(digits))
    return normalized


# =========================================================================
# 🧩 LOAD AND MERGE DATA
# =========================================================================
def load_all_data():
    # --- SHFB Supply ---
    SHFB_supply = pd.read_excel(SHFB_SUPPLY_FILE)
    SHFB_supply["Agency_No"] = SHFB_supply["No."].str[:4]
    SHFB_supply["Type"] = SHFB_supply["No."].str[4:7]
    SHFB_supply = SHFB_supply[~SHFB_supply["Name"].str.contains("MBL", na=False)]
    SHFB_supply["Cleaned_Address"] = SHFB_supply["Address"].apply(clean_address)
    SHFB_supply = SHFB_supply.groupby(["Agency_No", "Cleaned_Address"])["Total_20"].sum().reset_index()

    # --- SHFB Geo ---
    SHFB_Geo = pd.read_csv(SHFB_GEO_FILE)
    SHFB_Geo["Cleaned_Address"] = SHFB_Geo["Address"].apply(clean_address)
    SHFB_Geo_2 = SHFB_Geo.merge(SHFB_supply, on="Cleaned_Address", how="inner")
    SHFB_Geo_2 = SHFB_Geo_2[["Name", "Address", "Latitude", "Longitude", "Agency_No", "Total_20"]]
    SHFB_Geo = SHFB_Geo_2[["Name", "Total_20"]].rename(columns={"Total_20": "Avg_Monthly_Supply"})
    SHFB_Geo["Avg_Monthly_Supply"] /= 12

    # --- Agency–GEOID mapping ---
    Agency_GeoID = pd.read_excel(ODM_FILE)
    geo_info = pd.read_csv(GEO_INFO_FILE)
    geo_info.columns = geo_info.columns.str.strip()
    Agency_GeoID = Agency_GeoID.merge(
        geo_info[["tractid", "number_food_insecure"]],
        left_on="GEOID", right_on="tractid", how="left"
    )

    geo_map = pd.read_csv(GEO_MAP_FILE)
    geo_map.columns = geo_map.columns.str.strip()
    counties = [
        "Alamance","Alexander","Alleghany","Ashe","Caldwell","Caswell",
        "Davidson","Davie","Forsyth","Guilford","Iredell","Randolph",
        "Rockingham","Stokes","Surry","Watauga","Wilkes","Yadkin"
    ]
    
    county_col = "County_x"
    geo_map = (
        geo_map.assign(_county_clean=(
            geo_map[county_col].astype(str)
              .str.strip()
              .str.replace(r"\s*county$", "", case=False, regex=True)
              .str.title()
        ))
        .loc[lambda d: d["_county_clean"].isin(counties)]
        .drop(columns="_county_clean")
        .copy()
    )

    Agency_GeoID = Agency_GeoID.merge(geo_map, left_on="GEOID", right_on="GEOID_x", how="left")
    Agency_GeoID["Total_TravelTime"] = pd.to_numeric(Agency_GeoID["Total_TravelTime"], errors="coerce")
    Agency_GeoID["number_food_insecure"] = pd.to_numeric(Agency_GeoID["number_food_insecure"], errors="coerce")

    # --- Operating hours ---
    shfb_output = pd.read_csv(SHFB_OUTPUT_FILE)
    records = []
    for _, row in shfb_output.iterrows():
        agency = row.get("Name")
        raw_json = row.get("Model_Output")
        try:
            ai_output = json.loads(raw_json) if isinstance(raw_json, str) else raw_json
        except Exception:
            ai_output = None
        if not isinstance(ai_output, list):
            continue
        for entry in ai_output:
            week_list = normalize_week_list(entry.get("Week", []))
            day_name  = (entry.get("Day") or "").strip().title()
            open_str  = (entry.get("Opening_Hour") or "").strip()
            close_str = (entry.get("Closing_Hour") or "").strip()
            if not week_list or not day_name or not open_str:
                continue
            try:
                open_ts = parse_time_str(open_str)
            except Exception:
                continue
            if not close_str or close_str.lower() == "unknown":
                close_ts = open_ts + timedelta(hours=1)
            else:
                try:
                    close_ts = parse_time_str(close_str)
                except Exception:
                    close_ts = open_ts + timedelta(hours=1)
            hours = expand_to_hour_bins(open_ts, close_ts)
            for wk in week_list:
                for hr in hours:
                    records.append({"agency": agency, "week": int(wk), "day": day_name, "hour": int(hr)})

    hourly_df = pd.DataFrame.from_records(records)
    week = hourly_df.drop_duplicates(subset=["agency", "week", "day", "hour"])[["agency", "week", "day", "hour"]]
    return SHFB_Geo, Agency_GeoID, week, geo_map


# =========================================================================
# 🧮 ACCESS SCORE FUNCTION
# =========================================================================
def calculate_access_score(df, SHFB_Geo, week_value, day_value, hour_value,
                           urban_threshold=15, rural_threshold=25, beta=0.1):
    df_filtered = df[(df["week"] == week_value) &
                     (df["day"] == day_value) &
                     (df["hour"] == hour_value)].copy()
    if df_filtered.empty:
        return pd.DataFrame(), pd.DataFrame()  # ✅ Always two returns

    df_filtered["TravelTime_Threshold"] = np.where(df_filtered["Urban"] == 1, urban_threshold, rural_threshold)
    filtered_df_demand = df_filtered.dropna(subset=["Total_TravelTime"])
    filtered_df_demand = filtered_df_demand[
        filtered_df_demand["Total_TravelTime"] <= filtered_df_demand["TravelTime_Threshold"]
    ].copy()
    if filtered_df_demand.empty:
        return pd.DataFrame(), pd.DataFrame()  # ✅ Always two returns

    filtered_df_demand["exp_weight"] = np.exp(-beta * filtered_df_demand["Total_TravelTime"])
    filtered_df_demand["weighted_demand"] = (
        filtered_df_demand["exp_weight"] * filtered_df_demand["number_food_insecure"]
    )

    agency_demand = filtered_df_demand.groupby("Name", as_index=False)["weighted_demand"].sum()
    agency_food_insecure_sum = filtered_df_demand.groupby("Name", as_index=False)["number_food_insecure"].sum()

    agency_supply_demand = (
        agency_food_insecure_sum
        .merge(SHFB_Geo[["Name", "Avg_Monthly_Supply"]], on="Name", how="left")
        .merge(agency_demand, on="Name", how="left")
    )
    agency_supply_demand["R_A"] = (
        agency_supply_demand["Avg_Monthly_Supply"] / agency_supply_demand["weighted_demand"]
    ).replace([np.inf, -np.inf], 0).fillna(0)

    filtered_df = filtered_df_demand.merge(
        agency_supply_demand[["Name", "R_A"]], on="Name", how="left"
    )
    filtered_df["access_component"] = filtered_df["exp_weight"] * filtered_df["R_A"]

    # --- GEOID-level access score ---
    access_score_by_geoid = (
        filtered_df.groupby("GEOID", as_index=False)["access_component"].sum()
        .rename(columns={"access_component": "Access_Score"})
    )

    # --- GEOID–agency contributions ---
    agency_geo_contrib = (
        filtered_df.groupby(["GEOID", "Name"], as_index=False)
        .agg(Agency_Contribution=("access_component", "sum"))
    )

    # --- Add week/day/hour context ---
    for df_out in (access_score_by_geoid, agency_geo_contrib):
        df_out["week"] = week_value
        df_out["day"] = day_value
        df_out["hour"] = hour_value

    return access_score_by_geoid, agency_geo_contrib  # ✅ Always two outputs


# =========================================================================
# ⚙️ CONFIGURATION
# =========================================================================
URBAN_THRESHOLDS  = [10, 15, 20, 25]
RURAL_THRESHOLDS  = [20, 25, 30, 35, 40]
BETA_VALUES       = [0.1]
WEEKS             = [1, 2, 3, 4]
DAYS              = ["Monday","Tuesday","Wednesday","Thursday","Friday","Saturday","Sunday"]
HOURS             = range(0, 24)
OUTPUT_FILE       = "precomputed_access_scores_SHFB.parquet"


# =========================================================================
# 🚀 MAIN PRECOMPUTATION LOOP
# =========================================================================
if __name__ == "__main__":
    print("🔹 Loading datasets ...")
    SHFB_Geo, Agency_GeoID, week_df, geo_map = load_all_data()

    # Merge agency–geoID + schedule data
    filtered_df = Agency_GeoID.dropna(subset=["Total_TravelTime"]).merge(
        week_df, left_on="Name", right_on="agency", how="inner"
    )
    print(f"✅ Filtered dataset ready: {len(filtered_df):,} rows")

    # Run grid search
    results = []
    total_combos = len(URBAN_THRESHOLDS) * len(RURAL_THRESHOLDS) * len(BETA_VALUES) * len(WEEKS) * len(DAYS) * 24
    count = 0

    for u_th, r_th, beta, wk, day, hr in product(URBAN_THRESHOLDS, RURAL_THRESHOLDS, BETA_VALUES, WEEKS, DAYS, HOURS):
        count += 1
        print(f"({count}/{total_combos}) ▶ Week={wk}, Day={day}, Hour={hr}, Urban={u_th}, Rural={r_th}, β={beta}")

        geo_scores, agency_contrib = calculate_access_score(
            df=filtered_df,
            SHFB_Geo=SHFB_Geo,
            week_value=wk,
            day_value=day,
            hour_value=hr,
            urban_threshold=u_th,
            rural_threshold=r_th,
            beta=beta
        )

        if not geo_scores.empty and not agency_contrib.empty:
            # --- find top 3 agencies for each GEOID ---
            top_agency_json = (
                agency_contrib.sort_values(["GEOID", "Agency_Contribution"], ascending=[True, False])
                .groupby("GEOID")
                .apply(lambda d: d.head(3)[["Name", "Agency_Contribution"]].to_dict(orient="records"))
                .reset_index()
                .rename(columns={0: "Top_Agencies"})
            )

            # --- merge JSON back into main results ---
            geo_scores = geo_scores.merge(top_agency_json, on="GEOID", how="left")
            geo_scores["Top_Agencies"] = geo_scores["Top_Agencies"].apply(json.dumps)

            geo_scores["urban_threshold"] = u_th
            geo_scores["rural_threshold"] = r_th
            geo_scores["beta"] = beta
            results.append(geo_scores)

    # Save results
    if results:
        print("🔹 Concatenating results ...")
        final_df = pd.concat(results, ignore_index=True)
        final_df["GEOID"] = final_df["GEOID"].astype(str)
        print(f"✅ Computed {len(final_df):,} total rows")
        print(f"💾 Saving results to {OUTPUT_FILE} ...")
        final_df.to_parquet(OUTPUT_FILE, index=False)
        print("🎉 Precomputation complete.")
    else:
        print("⚠️ No results were computed. Check data availability or parameters.")
