# run_all.py
# 목적: 로컬 DB에서 대시보드용 JSON(Section1/2/3-Speed)을 생성
# 구조: 초보자도 따라갈 수 있도록 "3개 함수"로 분리 (기능/출력 동일)

import json
import os
import re
from datetime import date
from collections import defaultdict, Counter

import pymysql


# =========================
# helpers
# =========================
def yyyymmdd_from_dash(s: str) -> int:
    return int(s.replace("-", ""))


def today_yyyymmdd() -> int:
    return int(date.today().strftime("%Y%m%d"))


def load_config() -> dict:
    with open("config.json", "r", encoding="utf-8") as f:
        return json.load(f)


def load_standard_times() -> dict:
    path = os.path.join("..", "web", "data", "section2_standard_times.json")
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)


def ensure_out_dir() -> str:
    out_dir = os.path.join("..", "web", "data")
    os.makedirs(out_dir, exist_ok=True)
    return out_dir


def write_json(out_dir: str, filename: str, payload: dict) -> None:
    path = os.path.join(out_dir, filename)
    with open(path, "w", encoding="utf-8") as f:
        json.dump(payload, f, ensure_ascii=False, indent=2)
    print("✅ wrote", path)


def in_placeholders(n: int) -> str:
    # SQL IN (...) 바인딩용: "%s,%s,%s"
    return ",".join(["%s"] * n)


def assert_cfg(cfg: dict) -> None:
    # 최소한의 안전장치(초보자 실수 방지)
    required = [
        ("db", "host"),
        ("db", "port"),
        ("db", "user"),
        ("db", "password"),
        ("db", "database"),
        ("scope", "airlines"),
        ("scope", "date_from"),
        ("scope", "date_to"),
        ("work_types", "cabin_cleaning"),
    ]
    for a, b in required:
        if a not in cfg or b not in cfg[a]:
            raise ValueError(f"config.json 누락: {a}.{b}")

    if not isinstance(cfg["scope"]["airlines"], list) or len(cfg["scope"]["airlines"]) == 0:
        raise ValueError("config.json: scope.airlines 는 1개 이상 list 여야 합니다.")

    if not isinstance(cfg["work_types"]["cabin_cleaning"], list) or len(cfg["work_types"]["cabin_cleaning"]) == 0:
        raise ValueError("config.json: work_types.cabin_cleaning 는 1개 이상 list 여야 합니다.")


# =========================
# ETL: Section 1
# =========================
def etl_section1(conn, cfg, date_from: int, date_to: int, out_dir: str, airlines: list) -> None:
    """
    Section1:
      - section1_counts.json
      - section1_saved_points.json
      - section1_saved_stats.json
    """
    work_type_ids = set(cfg["work_types"]["cabin_cleaning"])

    # 표준시간
    standard_cfg = load_standard_times()
    default_standard_sec = standard_cfg["default_standard_sec"]
    standard_map = standard_cfg.get("by_airline_aircraft", {})

    ph_air = in_placeholders(len(airlines))

    # 1) work_id -> work_type 매핑(필터링용)
    with conn.cursor() as cur:
        cur.execute("SELECT ex_srl, work_type FROM rx_air_work WHERE work_type IS NOT NULL;")
        work_type_map = {r[0]: r[1] for r in cur.fetchall()}

    # 2) 항공사별 청소 건수 (기내청소 work_type만)
    sql_counts = f"""
    SELECT v.work_id, o.airline_code
    FROM v_work_time_clean v
    JOIN rx_air_operation o ON o.ex_srl = v.operation_srl
    WHERE v.quality='OK'
      AND v.work_yyyymmdd BETWEEN {date_from} AND {date_to}
      AND o.airline_code IN ({ph_air});
    """
    with conn.cursor() as cur:
        cur.execute(sql_counts, airlines)
        rows = cur.fetchall()

    counter = Counter()
    for work_id, airline in rows:
        if work_type_map.get(work_id) in work_type_ids:
            counter[airline] += 1

    write_json(
        out_dir,
        "section1_counts.json",
        {
            "range": {"from": str(date_from), "to": str(date_to)},
            "airlines": [{"code": k, "count": v} for k, v in counter.items()],
        },
    )

    # 3) 절감시간 points/stats
    sql_saved = f"""
    SELECT b.work_id, b.airline_code, b.aircraft_version_name, b.actual_sec
    FROM v_dashboard_base b
    WHERE b.quality='OK'
      AND b.work_yyyymmdd BETWEEN {date_from} AND {date_to}
      AND b.airline_code IN ({ph_air})
      AND b.actual_sec IS NOT NULL;
    """
    with conn.cursor() as cur:
        cur.execute(sql_saved, airlines)
        rows = cur.fetchall()

    points = []
    stats = defaultdict(list)

    for work_id, airline, aircraft, actual_sec in rows:
        if work_type_map.get(work_id) not in work_type_ids:
            continue
        key = f"{airline}|{aircraft}"
        std = standard_map.get(key, default_standard_sec)
        saved = std - actual_sec
        points.append({"airline": airline, "saved_sec": float(saved)})
        stats[airline].append(saved)

    write_json(
        out_dir,
        "section1_saved_points.json",
        {
            "range": {"from": str(date_from), "to": str(date_to)},
            "airlines": airlines,
            "points": points,
        },
    )

    write_json(
        out_dir,
        "section1_saved_stats.json",
        {
            "range": {"from": str(date_from), "to": str(date_to)},
            "stats": [
                {
                    "code": a,
                    "n": len(v),
                    "avg_saved_sec": (sum(v) / len(v)) if v else 0,
                    "min_saved_sec": min(v) if v else 0,
                    "max_saved_sec": max(v) if v else 0,
                }
                for a, v in stats.items()
            ],
        },
    )

    print("====Section1 ETL 완료")


# =========================
# ETL: Section 2
# =========================
def etl_section2(conn, cfg, date_from: int, date_to: int, out_dir: str, airlines: list) -> None:
    """
    Section2:
      - section2_aircraft_list.json
      - section2_aircraft_timeseries.json
    """
    work_type_ids = set(cfg["work_types"]["cabin_cleaning"])
    wt_list = sorted(work_type_ids)

    # 표준시간
    standard_cfg = load_standard_times()
    default_standard_sec = standard_cfg["default_standard_sec"]
    standard_map = standard_cfg.get("by_airline_aircraft", {})

    ph_air = in_placeholders(len(airlines))
    ph_wt = in_placeholders(len(wt_list))

    # params 순서 주의: airlines 먼저, wt_list 나중 (SQL의 IN 순서와 맞춰야 함)
    params = airlines + wt_list

    sql_list = f"""
    SELECT b.airline_code,
           COALESCE(NULLIF(b.aircraft_version_name,''),'Unknown') AS aircraft,
           COUNT(*) AS n
    FROM v_dashboard_base b
    JOIN rx_air_work w ON w.ex_srl = b.work_id
    WHERE b.quality='OK'
      AND b.work_yyyymmdd BETWEEN {date_from} AND {date_to}
      AND b.airline_code IN ({ph_air})
      AND w.work_type IN ({ph_wt})
    GROUP BY b.airline_code, aircraft;
    """

    sql_ts = f"""
    SELECT b.airline_code,
           COALESCE(NULLIF(b.aircraft_version_name,''),'Unknown') AS aircraft,
           b.work_yyyymmdd,
           COUNT(*),
           ROUND(AVG(b.actual_sec),1),
           MIN(b.actual_sec),
           MAX(b.actual_sec)
    FROM v_dashboard_base b
    JOIN rx_air_work w ON w.ex_srl = b.work_id
    WHERE b.quality='OK'
      AND b.work_yyyymmdd BETWEEN {date_from} AND {date_to}
      AND b.airline_code IN ({ph_air})
      AND w.work_type IN ({ph_wt})
    GROUP BY b.airline_code, aircraft, b.work_yyyymmdd;
    """

    with conn.cursor() as cur:
        cur.execute(sql_list, params)
        rows_list = cur.fetchall()

        cur.execute(sql_ts, params)
        rows_ts = cur.fetchall()

    by_airline = defaultdict(list)
    for a, ac, n in rows_list:
        std = standard_map.get(f"{a}|{ac}", default_standard_sec)
        by_airline[a].append({"aircraft": ac, "n": int(n), "standard_sec": std})

    write_json(
        out_dir,
        "section2_aircraft_list.json",
        {
            "range": {"from": str(date_from), "to": str(date_to)},
            "airlines": airlines,
            "aircraft_by_airline": dict(by_airline),
        },
    )

    series = defaultdict(list)
    for a, ac, d, n, avg, mn, mx in rows_ts:
        std = standard_map.get(f"{a}|{ac}", default_standard_sec)
        series[f"{a}|{ac}"].append(
            {
                "yyyymmdd": int(d),
                "n": int(n),
                "avg_actual_sec": float(avg),
                "min_actual_sec": int(mn),
                "max_actual_sec": int(mx),
                "standard_sec": std,
            }
        )

    write_json(
        out_dir,
        "section2_aircraft_timeseries.json",
        {
            "range": {"from": str(date_from), "to": str(date_to)},
            "series": dict(series),
        },
    )

    print("====Section2 ETL 완료")


# =========================
# ETL: Section 3-Speed
# =========================
def etl_section3_speed(conn, cfg, date_from: int, date_to: int, out_dir: str, airlines: list) -> None:
    """
    Section3-Speed:
      - section3_speed_rows.json

    핵심:
      - SQL에서 이미 "role_label별 총 시간(백업 포함)"을 만들고,
      - Python에서는 process/zone 매핑만 해서 JSON으로 저장
    """
    work_type_ids = set(cfg["work_types"]["cabin_cleaning"])
    wt_list = sorted(work_type_ids)

    ph_air = in_placeholders(len(airlines))
    ph_wt = in_placeholders(len(wt_list))

    # config의 exclude_labels를 SQL에 반영 (없으면 기본)
    pr = cfg.get("process_rules", {})
    exclude_labels = pr.get("exclude_labels", ["무효", "OJT"])
    # IN (%s, %s) 형태로 바인딩
    ph_ex = in_placeholders(len(exclude_labels))

    sql_s3_speed = f"""
    WITH target_work AS (
        SELECT
            w.ex_srl,
            w.date,
            w.title,
            o.airline_code
        FROM rx_air_work w
        JOIN rx_air_operation o ON o.ex_srl = w.operation_srl
        WHERE
            w.work_type IN ({ph_wt})
            AND w.date BETWEEN %s AND %s
            AND o.airline_code IN ({ph_air})
    ),

    log_norm AS (
        SELECT
            dl.work_srl,

            /* 백업은 wdl_group_label, 메인은 wdl_label */
            COALESCE(dl.wdl_group_label, dl.wdl_label) AS group_label,

            COALESCE(dl.member_srl, wm.member_srl) AS member_srl,

            CASE
                WHEN dl.wdl_duration REGEXP '^[0-9]+$' THEN CAST(dl.wdl_duration AS UNSIGNED)
                WHEN dl.wdl_duration REGEXP '^[0-9]{{1,2}}:[0-9]{{2}}:[0-9]{{2}}$' THEN TIME_TO_SEC(dl.wdl_duration)
                WHEN dl.wdl_duration REGEXP '^[0-9]{{1,2}}:[0-9]{{2}}$' THEN TIME_TO_SEC(CONCAT('00:', dl.wdl_duration))
                ELSE 0
            END AS duration_sec,

            dl.wdl_label,
            dl.wdl_group_label
        FROM rx_air_work_duration_log dl
        LEFT JOIN rx_air_work_member wm
          ON wm.wm_srl = dl.wm_srl
        JOIN target_work tw
          ON tw.ex_srl = dl.work_srl
        WHERE
            /* 공정 라벨만 가져오기 */
            (
              dl.wdl_label IN (
                '소닉1','소닉백업존1','소닉2','소닉백업존2','소닉3','소닉백업존3',
                '소닉4','소닉백업존4','소닉5','소닉백업존5','소닉6','소닉백업존6',
                '라바','라바백업','로보캅','로보캅백업'
              )
              OR dl.wdl_group_label IN ('소닉1','소닉2','소닉3','소닉4','소닉5','소닉6','라바','로보캅')
            )
            /* 제외 라벨 */
            AND dl.wdl_label NOT IN ({ph_ex})
    ),

    main_map AS (
        /* 메인 담당자: wdl_group_label IS NULL 이고 메인 라벨인 사람 */
        SELECT
            ln.work_srl,
            ln.group_label,
            MIN(ln.member_srl) AS main_member_srl
        FROM log_norm ln
        WHERE
            ln.wdl_group_label IS NULL
            AND ln.wdl_label IN ('소닉1','소닉2','소닉3','소닉4','소닉5','소닉6','라바','로보캅')
        GROUP BY
            ln.work_srl,
            ln.group_label
    ),

    agg AS (
        /* 백업 포함 전체 시간을 group_label 단위로 합산 */
        SELECT
            ln.work_srl,
            ln.group_label,
            SUM(ln.duration_sec) AS total_sec
        FROM log_norm ln
        WHERE
            ln.group_label IN ('소닉1','소닉2','소닉3','소닉4','소닉5','소닉6','라바','로보캅')
        GROUP BY
            ln.work_srl,
            ln.group_label
    )

    SELECT
        tw.date,
        tw.airline_code,
        tw.title AS flight_title,
        a.group_label AS role_label,
        mm.main_member_srl,
        m.user_id   AS member_user_id,
        COALESCE(NULLIF(m.nick_name,''), m.user_name) AS member_name,
        a.total_sec,
        ROUND(a.total_sec / 60, 1) AS total_min
    FROM target_work tw
    JOIN agg a
      ON a.work_srl = tw.ex_srl
    JOIN main_map mm
      ON mm.work_srl = a.work_srl
     AND mm.group_label = a.group_label
    LEFT JOIN rx_member m
      ON m.member_srl = mm.main_member_srl
    ORDER BY
        tw.date ASC,
        flight_title ASC;
    """

    # 파라미터 순서 = (wt_list...) + date_from + date_to + (airlines...) + (exclude_labels...)
    params_s3_speed = wt_list + [str(date_from), str(date_to)] + airlines + exclude_labels

    with conn.cursor() as cur:
        cur.execute(sql_s3_speed, params_s3_speed)
        speed_rows = cur.fetchall()

    def role_to_process_zone(role_label: str):
        role_label = (role_label or "").strip()

        # 소닉N
        m = re.match(r"^소닉(\d+)$", role_label)
        if m:
            return "소닉", m.group(1)

        # 소닉* 기타 라벨은 일단 소닉으로 묶고 zone=0
        if role_label.startswith("소닉"):
            return "소닉", "0"

        if role_label.startswith("라바"):
            return "라바", "0"

        # 로보캅 (DB에서 role_label이 '로보캅'으로 나오도록 설계)
        return "로보캅", "0"

    rows_out = []
    for d, airline_code, flight_title, role_label, msrl, user_id, name, total_sec, total_min in speed_rows:
        process, zone = role_to_process_zone(role_label)

        rows_out.append(
            {
                "date": str(d),
                "airline": airline_code,
                "flight_title": flight_title,
                "role_label": role_label,      # 소닉1/2/.. 라바/로보캅
                "process": process,            # 소닉/라바/로보캅
                "zone": str(zone),             # 소닉은 1~6, 그 외 0
                "member_srl": int(msrl),
                "member_user_id": user_id or "",
                "member_name": name or "",
                "time_sec": int(total_sec or 0),
                "backup_sec_attached": 0,      # 이미 SQL에서 합산했으므로 0
                "total_min": float(total_min or 0),
            }
        )

    write_json(
        out_dir,
        "section3_speed_rows.json",
        {"range": {"from": str(date_from), "to": str(date_to)}, "rows": rows_out},
    )

    print("====Section3-Speed ETL 완료")


# =========================
# main
# =========================
def main():
    print("### run_all.py 시작됨 ###")

    cfg = load_config()
    assert_cfg(cfg)

    db = cfg["db"]
    airlines = cfg["scope"]["airlines"]

    date_from = yyyymmdd_from_dash(cfg["scope"]["date_from"])
    date_to = (
        today_yyyymmdd()
        if cfg["scope"]["date_to"] == "TODAY"
        else yyyymmdd_from_dash(cfg["scope"]["date_to"])
    )

    out_dir = ensure_out_dir()

    conn = pymysql.connect(
        host=db["host"],
        port=int(db["port"]),
        user=db["user"],
        password=db["password"],
        database=db["database"],
        charset="utf8mb4",
        autocommit=True,
    )

    try:
        etl_section1(conn, cfg, date_from, date_to, out_dir, airlines)
        etl_section2(conn, cfg, date_from, date_to, out_dir, airlines)
        etl_section3_speed(conn, cfg, date_from, date_to, out_dir, airlines)

        print("### run_all.py 끝까지 실행됨 ###")
    finally:
        conn.close()


if __name__ == "__main__":
    main()
