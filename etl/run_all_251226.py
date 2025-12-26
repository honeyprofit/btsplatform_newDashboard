import json
import os
from datetime import date
from collections import defaultdict, Counter
import re

import pymysql


# =========================
# helpers
# =========================
def yyyymmdd_from_dash(s: str) -> int:
    return int(s.replace("-", ""))


def today_yyyymmdd() -> int:
    return int(date.today().strftime("%Y%m%d"))


def load_config():
    with open("config.json", "r", encoding="utf-8") as f:
        return json.load(f)


def load_standard_times():
    path = os.path.join("..", "web", "data", "section2_standard_times.json")
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)


def ensure_out_dir():
    out_dir = os.path.join("..", "web", "data")
    os.makedirs(out_dir, exist_ok=True)
    return out_dir


def write_json(out_dir, filename, payload):
    path = os.path.join(out_dir, filename)
    with open(path, "w", encoding="utf-8") as f:
        json.dump(payload, f, ensure_ascii=False, indent=2)
    print("✅ wrote", path)


def in_placeholders(n: int) -> str:
    return ",".join(["%s"] * n)


# =========================
# main ETL
# =========================
def main():
    print("### run_all.py 시작됨 ###")

    cfg = load_config()
    db = cfg["db"]

    airlines = cfg["scope"]["airlines"]
    work_type_ids = set(cfg["work_types"]["cabin_cleaning"])

    date_from = yyyymmdd_from_dash(cfg["scope"]["date_from"])
    date_to = (
        today_yyyymmdd()
        if cfg["scope"]["date_to"] == "TODAY"
        else yyyymmdd_from_dash(cfg["scope"]["date_to"])
    )

    out_dir = ensure_out_dir()

    # 표준시간
    standard_cfg = load_standard_times()
    default_standard_sec = standard_cfg["default_standard_sec"]
    standard_map = standard_cfg.get("by_airline_aircraft", {})

    # 공정 규칙
    pr = cfg.get("process_rules", {})
    exclude_labels = set(pr.get("exclude_labels", ["무효", "OJT"]))
    sonic_prefixes = pr.get("sonic_prefixes", ["소닉"])
    lava_prefixes = pr.get("lava_prefixes", ["라바"])
    robocop_prefixes = pr.get("robocop_prefixes", ["베큠", "폐기물"])

    ph = in_placeholders(len(airlines))

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
        # =================================================
        # work_id -> work_type 매핑
        # =================================================
        with conn.cursor() as cur:
            cur.execute(
                "SELECT ex_srl, work_type FROM rx_air_work WHERE work_type IS NOT NULL;"
            )
            work_type_map = {r[0]: r[1] for r in cur.fetchall()}

        # =================================================
        # Section 1-1: 항공사별 청소 건수
        # =================================================
        sql = f"""
        SELECT v.work_id, o.airline_code
        FROM v_work_time_clean v
        JOIN rx_air_operation o ON o.ex_srl = v.operation_srl
        WHERE v.quality='OK'
          AND v.work_yyyymmdd BETWEEN {date_from} AND {date_to}
          AND o.airline_code IN ({ph});
        """
        with conn.cursor() as cur:
            cur.execute(sql, airlines)
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

        # =================================================
        # Section 1-2 / 1-3: 절감시간
        # =================================================
        sql = f"""
        SELECT b.work_id, b.airline_code, b.aircraft_version_name, b.actual_sec
        FROM v_dashboard_base b
        WHERE b.quality='OK'
          AND b.work_yyyymmdd BETWEEN {date_from} AND {date_to}
          AND b.airline_code IN ({ph})
          AND b.actual_sec IS NOT NULL;
        """
        with conn.cursor() as cur:
            cur.execute(sql, airlines)
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
                        "avg_saved_sec": sum(v) / len(v),
                        "min_saved_sec": min(v),
                        "max_saved_sec": max(v),
                    }
                    for a, v in stats.items()
                ],
            },
        )

        # =================================================
        # Section 2: 항공기별 통계
        # =================================================
        wt_list = sorted(work_type_ids)
        params = airlines + wt_list

        sql_list = f"""
        SELECT b.airline_code,
               COALESCE(NULLIF(b.aircraft_version_name,''),'Unknown') AS aircraft,
               COUNT(*) AS n
        FROM v_dashboard_base b
        JOIN rx_air_work w ON w.ex_srl = b.work_id
        WHERE b.quality='OK'
          AND b.work_yyyymmdd BETWEEN {date_from} AND {date_to}
          AND b.airline_code IN ({ph})
          AND w.work_type IN ({in_placeholders(len(wt_list))})
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
          AND b.airline_code IN ({ph})
          AND w.work_type IN ({in_placeholders(len(wt_list))})
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

        # =================================================
        # Section 3-Speed: SQL에서 이미 'role_label별 총 시간(백업 포함)'을 만든 결과를 그대로 JSON으로 출력
        # - role_label: 소닉1~6 / 라바 / 로보캅
        # - process: 소닉/라바/로보캅
        # - zone: 소닉N이면 "N", 그 외 "0"
        # =================================================

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
                w.work_type IN ({in_placeholders(len(wt_list))})
                AND w.date BETWEEN %s AND %s
                AND o.airline_code IN ({ph})
        ),

        log_norm AS (
            SELECT
                dl.work_srl,
                /* 백업은 group_label, 메인은 label */
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
                (dl.wdl_label IN (
                    '소닉1','소닉백업존1','소닉2','소닉백업존2','소닉3','소닉백업존3',
                    '소닉4','소닉백업존4','소닉5','소닉백업존5','소닉6','소닉백업존6',
                    '라바','라바백업','로보캅','로보캅백업'
                 )
                 OR dl.wdl_group_label IN ('소닉1','소닉2','소닉3','소닉4','소닉5','소닉6','라바','로보캅'))
                AND dl.wdl_label NOT IN ('무효','OJT')
        ),

        main_map AS (
            /* 메인 담당자: wdl_group_label NULL 이고 메인 라벨인 사람 */
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

        params_s3_speed = wt_list + [str(date_from), str(date_to)] + airlines

        with conn.cursor() as cur:
            cur.execute(sql_s3_speed, params_s3_speed)
            speed_rows = cur.fetchall()

        def role_to_process_zone(role_label: str):
            role_label = (role_label or "").strip()
            # 소닉N
            m = re.match(r"^소닉(\d+)$", role_label)
            if m:
                return "소닉", m.group(1)
            if role_label.startswith("소닉"):
                # 예외 라벨은 일단 소닉으로 묶고 zone=0
                return "소닉", "0"
            if role_label.startswith("라바"):
                return "라바", "0"
            # 로보캅 (DB에서는 '로보캅'으로 나옴)
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
                    "backup_sec_attached": 0,      # SQL에서 이미 합산했으니 0으로 둠
                    "total_min": float(total_min or 0),
                }
            )

        write_json(
            out_dir,
            "section3_speed_rows.json",
            {
                "range": {"from": str(date_from), "to": str(date_to)},
                "rows": rows_out,
            },
        )


        print("### run_all.py 끝까지 실행됨 ###")

    finally:
        conn.close()


if __name__ == "__main__":
    main()
