import json
import os
from datetime import date
from collections import defaultdict, Counter

import pymysql


# -------------------------
# helpers
# -------------------------
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


# -------------------------
# main ETL
# -------------------------
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

    standard_cfg = load_standard_times()
    default_standard_sec = standard_cfg["default_standard_sec"]
    standard_map = standard_cfg.get("by_airline_aircraft", {})

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
        # -------------------------------------------------
        # work_id -> work_type 매핑 (핵심)
        # -------------------------------------------------
        sql_work_types = """
        SELECT ex_srl, work_type
        FROM rx_air_work
        WHERE work_type IS NOT NULL;
        """
        with conn.cursor() as cur:
            cur.execute(sql_work_types)
            work_type_map = {r[0]: r[1] for r in cur.fetchall()}

        # =================================================
        # Section 1-1: 항공사별 청소 건수 (Python 집계)
        # =================================================
        sql_raw = f"""
        SELECT
          v.work_id,
          o.airline_code
        FROM v_work_time_clean v
        JOIN rx_air_operation o ON o.ex_srl = v.operation_srl
        WHERE v.quality='OK'
          AND v.work_yyyymmdd BETWEEN {date_from} AND {date_to}
          AND o.airline_code IN ({ph});
        """

        with conn.cursor() as cur:
            cur.execute(sql_raw, airlines)
            raw_rows = cur.fetchall()

        counter = Counter()
        for work_id, airline in raw_rows:
            if work_type_map.get(work_id) in work_type_ids:
                counter[airline] += 1

        payload_counts = {
            "range": {"from": str(date_from), "to": str(date_to)},
            "airlines": [
                {"code": code, "count": cnt}
                for code, cnt in counter.items()
            ],
        }
        write_json(out_dir, "section1_counts.json", payload_counts)

        # =================================================
        # Section 1-2: 절감시간 포인트 (Python 필터)
        # =================================================
        sql_points_raw = f"""
        SELECT
            b.work_id,
            b.airline_code,
            b.aircraft_version_name,
            b.actual_sec
        FROM v_dashboard_base b
        WHERE b.quality='OK'
            AND b.work_yyyymmdd BETWEEN {date_from} AND {date_to}
            AND b.airline_code IN ({ph})
            AND b.actual_sec IS NOT NULL
            # AND b.aircraft_version_name IS NOT NULL
            # AND b.aircraft_version_name <> '';
        """


        with conn.cursor() as cur:
            cur.execute(sql_points_raw, airlines)
            rows = cur.fetchall()

        points = []
        for work_id, airline, aircraft, actual_sec in rows:
            # 기내청소 work_type만 통과
            if work_type_map.get(work_id) not in work_type_ids:
                continue

            # JSON 표준시간 선택
            key = f"{airline}|{aircraft}"
            standard_sec = standard_map.get(key, default_standard_sec)

            saved_sec = standard_sec - actual_sec
            points.append({"airline": airline, "saved_sec": float(saved_sec)})


        payload_points = {
            "range": {"from": str(date_from), "to": str(date_to)},
            "airlines": airlines,
            "points": points,
        }
        write_json(out_dir, "section1_saved_points.json", payload_points)

        # =================================================
        # Section 1-3: 절감시간 요약 (Python 집계)
        # =================================================
        stats_map = defaultdict(list)

        for work_id, airline, aircraft, actual_sec in rows:
            if work_type_map.get(work_id) not in work_type_ids:
                continue

            key = f"{airline}|{aircraft}"
            standard_sec = standard_map.get(key, default_standard_sec)

            saved_sec = standard_sec - actual_sec
            stats_map[airline].append(saved_sec)


        payload_stats = {
            "range": {"from": str(date_from), "to": str(date_to)},
            "stats": [],
        }

        for airline, vals in stats_map.items():
            payload_stats["stats"].append(
                {
                    "code": airline,
                    "n": len(vals),
                    "avg_saved_sec": sum(vals) / len(vals),
                    "min_saved_sec": min(vals),
                    "max_saved_sec": max(vals),
                }
            )

        write_json(out_dir, "section1_saved_stats.json", payload_stats)

        print("====Section1 ETL 완료")

        # =================================================
        # Section 2
        # =================================================

        sql_s2_list = f"""
        SELECT
            b.airline_code,
            COALESCE(NULLIF(b.aircraft_version_name, ''), 'Unknown') AS aircraft,
            COUNT(*) AS n
        FROM v_dashboard_base b
        JOIN rx_air_work w
        ON b.work_id = w.ex_srl
        WHERE b.quality='OK'
            AND b.work_yyyymmdd BETWEEN {date_from} AND {date_to}
            AND b.airline_code IN ({ph})
            AND b.actual_sec IS NOT NULL
            AND w.work_type IN ({in_placeholders(len(work_type_ids))})
        GROUP BY b.airline_code, aircraft
        ORDER BY b.airline_code, n DESC;
        """


        sql_s2_ts = f"""
        SELECT
            b.airline_code,
            COALESCE(NULLIF(b.aircraft_version_name, ''), 'Unknown') AS aircraft,
            b.work_yyyymmdd,
            COUNT(*) AS n,
            ROUND(AVG(b.actual_sec), 1) AS avg_actual_sec,
            MIN(b.actual_sec) AS min_actual_sec,
            MAX(b.actual_sec) AS max_actual_sec
        FROM v_dashboard_base b
        JOIN rx_air_work w
        ON b.work_id = w.ex_srl
        WHERE b.quality='OK'
            AND b.work_yyyymmdd BETWEEN {date_from} AND {date_to}
            AND b.airline_code IN ({ph})
            AND b.actual_sec IS NOT NULL
            AND w.work_type IN ({in_placeholders(len(work_type_ids))})
        GROUP BY b.airline_code, aircraft, b.work_yyyymmdd
        ORDER BY b.airline_code, aircraft, b.work_yyyymmdd;
        """


        with conn.cursor() as cur:
            # 기내청소 work_type 파라미터 준비
            work_type_list = sorted(list(work_type_ids))
            params = airlines + work_type_list

            # 기종 목록
            cur.execute(sql_s2_list, params)
            rows_list = cur.fetchall()

            # 시계열
            cur.execute(sql_s2_ts, params)
            rows_ts = cur.fetchall()

        by_airline = defaultdict(list)
        for r in rows_list:
            key = f"{r[0]}|{r[1]}"
            std_sec = standard_map.get(key, default_standard_sec)
            by_airline[r[0]].append(
                {"aircraft": r[1], "n": int(r[2]), "standard_sec": std_sec}
            )

        write_json(
            out_dir,
            "section2_aircraft_list.json",
            {
                "range": {"from": str(date_from), "to": str(date_to)},
                "airlines": airlines,
                "aircraft_by_airline": dict(by_airline),
            },
        )

        ts_map = defaultdict(list)
        for r in rows_ts:
            key = f"{r[0]}|{r[1]}"
            std_sec = standard_map.get(key, default_standard_sec)
            ts_map[key].append(
                {
                    "yyyymmdd": int(r[2]),
                    "n": int(r[3]),
                    "avg_actual_sec": float(r[4]),
                    "min_actual_sec": int(r[5]),
                    "max_actual_sec": int(r[6]),
                    "standard_sec": std_sec,
                }
            )

        write_json(
            out_dir,
            "section2_aircraft_timeseries.json",
            {
                "range": {"from": str(date_from), "to": str(date_to)},
                "series": dict(ts_map),
            },
        )

        # =================================================
        # Section 2-Process: 항공사별 공정(소닉/라바/로보캅) 일자별 평균
        # - source: rx_air_work_duration_log (wdl_label, wdl_duration=mm:ss)
        # - join: duration_log -> work_member -> air_work -> operation
        # - filter: cabin_cleaning work_type + date range
        # - metric: (공정 총합 초) / (그날 비행기 수) => 비행기 1대당 평균(분)
        # =================================================

        rules = cfg.get("process_rules", {})
        exclude_labels = set(rules.get("exclude_labels", ["무효", "OJT"]))
        sonic_prefixes = rules.get("sonic_prefixes", ["소닉"])
        lava_prefixes = rules.get("lava_prefixes", ["라바"])
        robocop_prefixes = rules.get("robocop_prefixes", ["베큠", "폐기물"])

        def mmss_to_seconds(s: str) -> int:
            # "12:43" -> 763초
            try:
                mm, ss = s.split(":")
                return int(mm) * 60 + int(ss)
            except Exception:
                return 0

        def classify_process(label: str) -> str:
            # 제외 라벨은 None 처리
            if label in exclude_labels:
                return None

            for p in sonic_prefixes:
                if label.startswith(p):
                    return "소닉"
            for p in lava_prefixes:
                if label.startswith(p):
                    return "라바"
            for p in robocop_prefixes:
                if label.startswith(p):
                    return "로보캅"
            return "기타"

        # 1) 날짜별 비행기 수(분모) 구하기: ops_count[(airline, yyyymmdd)] = distinct operation_srl
        sql_ops = f"""
        SELECT
          o.airline_code,
          wm.work_date,
          COUNT(DISTINCT w.operation_srl) AS ops
        FROM rx_air_work_member wm
        JOIN rx_air_work w ON w.ex_srl = wm.work_srl
        JOIN rx_air_operation o ON o.ex_srl = w.operation_srl
        WHERE w.work_type IN ({in_placeholders(len(work_type_ids))})
          AND wm.work_date BETWEEN %s AND %s
          AND o.airline_code IN ({ph})
        GROUP BY o.airline_code, wm.work_date;
        """

        work_type_list = sorted(list(work_type_ids))
        params_ops = work_type_list + [str(date_from), str(date_to)] + airlines

        ops_count = {}
        with conn.cursor() as cur:
            cur.execute(sql_ops, params_ops)
            for airline, work_date, ops in cur.fetchall():
                ops_count[(airline, int(work_date))] = int(ops) if ops else 0

        # 2) duration_log에서 공정별 총 시간(분자) 합치기
        # 2) 작업자 단위로 라벨을 모아서 공정 1개로 확정한 뒤,
        #    (공정별 total_time 합) / (공정 참여자 수) 계산

        sql_proc = f"""
        SELECT
          o.airline_code,
          wm.work_date,
          w.operation_srl,
          wm.wm_srl,
          wm.total_time,
          d.wdl_label
        FROM rx_air_work_duration_log d
        JOIN rx_air_work_member wm ON wm.wm_srl = d.wm_srl
        JOIN rx_air_work w ON w.ex_srl = wm.work_srl
        JOIN rx_air_operation o ON o.ex_srl = w.operation_srl
        WHERE w.work_type IN ({in_placeholders(len(work_type_ids))})
          AND wm.work_date BETWEEN %s AND %s
          AND o.airline_code IN ({ph});
        """

        params_proc = work_type_list + [str(date_from), str(date_to)] + airlines

        # labels_by_member[(airline, yyyymmdd, wm_srl)] = set(labels)
        labels_by_member = defaultdict(set)
        # info_by_member[(airline, yyyymmdd, wm_srl)] = {"total_time": int, "operation_srl": int}
        info_by_member = {}

        with conn.cursor() as cur:
            cur.execute(sql_proc, params_proc)
            for airline, work_date, op_srl, wm_srl, total_time, label in cur.fetchall():
                if not airline or not work_date or not wm_srl:
                    continue
                yyyymmdd = int(work_date)
                key_m = (airline, yyyymmdd, int(wm_srl))
                if label:
                    labels_by_member[key_m].add(str(label).strip())
                # total_time은 한 wm_srl에 대해 동일해야 함(중복 row여도 같은 값)
                if key_m not in info_by_member:
                    info_by_member[key_m] = {
                        "total_time": int(total_time) if total_time else 0,
                        "operation_srl": int(op_srl) if op_srl else None,
                    }

        def pick_process_from_labels(labels: set) -> str:
            # exclude가 하나라도 있으면 그 라벨은 무시(=labels에서 제거)
            filtered = {lb for lb in labels if lb and lb not in exclude_labels}

            # 우선순위: 소닉 > 라바 > 로보캅 > 기타
            for lb in filtered:
                for p in sonic_prefixes:
                    if lb.startswith(p):
                        return "소닉"
            for lb in filtered:
                for p in lava_prefixes:
                    if lb.startswith(p):
                        return "라바"
            for lb in filtered:
                for p in robocop_prefixes:
                    if lb.startswith(p):
                        return "로보캅"
            return "기타"

        # sum_sec[(airline, date, proc)] = total_time 합(초)
        sum_sec = defaultdict(int)
        # members[(airline, date, proc)] = set(wm_srl)
        members = defaultdict(set)

        for key_m, labels in labels_by_member.items():
            airline, yyyymmdd, wm_srl = key_m
            info = info_by_member.get(key_m)
            if not info:
                continue
            tsec = info.get("total_time", 0)
            if tsec <= 0:
                continue

            proc = pick_process_from_labels(labels)
            if proc in ["소닉", "라바", "로보캅"]:
                key_p = (airline, yyyymmdd, proc)
                sum_sec[key_p] += tsec
                members[key_p].add(wm_srl)

        # 3) 시계열 만들기: avg_min = (sum_sec / member_cnt) / 60
        series = defaultdict(list)
        total_sec = defaultdict(int)
        total_members = defaultdict(int)

        for (airline, yyyymmdd, proc), sec in sorted(sum_sec.items()):
            mcnt = len(members[(airline, yyyymmdd, proc)])
            if mcnt <= 0:
                continue
            avg_min = (sec / mcnt) / 60.0
            key = f"{airline}|{proc}"
            series[key].append(
                {
                    "yyyymmdd": int(yyyymmdd),
                    "members": int(mcnt),
                    "sum_sec": int(sec),
                    "avg_min": round(avg_min, 2),
                }
            )
            total_sec[key] += sec
            total_members[key] += mcnt

        period_avg = {}
        for key in ["%s|%s" % (a, p) for a in airlines for p in ["소닉", "라바", "로보캅"]]:
            mcnt = total_members.get(key, 0)
            sec = total_sec.get(key, 0)
            period_avg[key] = round((sec / mcnt) / 60.0, 2) if mcnt > 0 else None

        write_json(
            out_dir,
            "section2_process_timeseries.json",
            {
                "range": {"from": str(date_from), "to": str(date_to)},
                "series": dict(series),
                "period_avg_min": period_avg,
                "meta": {
                    "exclude_labels": sorted(list(exclude_labels)),
                    "sonic_prefixes": sonic_prefixes,
                    "lava_prefixes": lava_prefixes,
                    "robocop_prefixes": robocop_prefixes,
                    "definition": "avg_min = (sum of wm.total_time for members classified to process) / (distinct member count)",
                },
            },
        )


        print("====Section2-Process ETL 완료")


        print("====Section2 ETL 완료")

        # =================================================
        # Section 3: 작업자별 공정 수행 "항공기 수"(= COUNT DISTINCT work_id)
        # - 탭: ALL/HH/RF/8M (프론트에서 처리)
        # - 이름: rx_member.nick_name 우선, 없으면 user_name
        # - 공정 분류: labels(set) -> 소닉/라바/로보캅
        # =================================================

        rules = cfg.get("process_rules", {})
        exclude_labels = set(rules.get("exclude_labels", ["무효", "OJT"]))
        sonic_prefixes = rules.get("sonic_prefixes", ["소닉", "Y좌석", "C좌석", "B좌석청소", "좌석청소"])
        lava_prefixes = rules.get("lava_prefixes", ["라바"])
        robocop_prefixes = rules.get("robocop_prefixes", ["베큠", "폐기물", "비우기", "닦기", "담요"])

        def pick_process_from_labels(labels: set) -> str:
            filtered = {lb for lb in labels if lb and lb not in exclude_labels}
            # 우선순위: 소닉 > 라바 > 로보캅
            for lb in filtered:
                for p in sonic_prefixes:
                    if lb.startswith(p):
                        return "소닉"
            for lb in filtered:
                for p in lava_prefixes:
                    if lb.startswith(p):
                        return "라바"
            for lb in filtered:
                for p in robocop_prefixes:
                    if lb.startswith(p):
                        return "로보캅"
            return None

        # 1) 작업자(wm_srl) 단위로 라벨 모으기 + (airline, work_id, member_srl) 보관
        sql_s3_raw = f"""
        SELECT
          o.airline_code,
          wm.work_date,
          w.ex_srl AS work_id,
          wm.wm_srl,
          wm.member_srl,
          d.wdl_label
        FROM rx_air_work_duration_log d
        JOIN rx_air_work_member wm ON wm.wm_srl = d.wm_srl
        JOIN rx_air_work w ON w.ex_srl = wm.work_srl
        JOIN rx_air_operation o ON o.ex_srl = w.operation_srl
        WHERE w.work_type IN ({in_placeholders(len(work_type_ids))})
          AND wm.work_date BETWEEN %s AND %s
          AND o.airline_code IN ({ph})
          AND d.wdl_label NOT IN ('무효','OJT');
        """

        work_type_list = sorted(list(work_type_ids))
        params_s3 = work_type_list + [str(date_from), str(date_to)] + airlines

        labels_by_wm = defaultdict(set)
        wm_info = {}  # (airline, wm_srl) -> {member_srl, work_id}

        with conn.cursor() as cur:
            cur.execute(sql_s3_raw, params_s3)
            for airline, work_date, work_id, wm_srl, member_srl, label in cur.fetchall():
                if not airline or not wm_srl or not member_srl or not work_id:
                    continue
                k = (airline, int(wm_srl))
                if label:
                    labels_by_wm[k].add(str(label).strip())
                if k not in wm_info:
                    wm_info[k] = {"member_srl": int(member_srl), "work_id": int(work_id)}

        # 2) (airline, member_srl, process) 별로 DISTINCT work_id 세기
        aircraft_set = defaultdict(set)  # key -> set(work_id)

        for (airline, wm_srl), labels in labels_by_wm.items():
            info = wm_info.get((airline, wm_srl))
            if not info:
                continue
            proc = pick_process_from_labels(labels)
            if proc not in ("소닉", "라바", "로보캅"):
                continue
            member_srl = info["member_srl"]
            work_id = info["work_id"]
            aircraft_set[(airline, member_srl, proc)].add(work_id)

        # 3) 이름 매핑: rx_member
        sql_member = f"""
        SELECT
          member_srl,
          nick_name,
          user_name
        FROM rx_member
        WHERE member_srl IN ({in_placeholders(len(set([k[1] for k in aircraft_set.keys()])))});
        """

        member_ids = sorted(list(set([k[1] for k in aircraft_set.keys()])))
        member_name_map = {}
        if member_ids:
            with conn.cursor() as cur:
                cur.execute(sql_member, member_ids)
                for msrl, nick, uname in cur.fetchall():
                    name = (nick or "").strip() or (uname or "").strip() or f"ID_{msrl}"
                    member_name_map[int(msrl)] = name

        # 4) rows 만들기
        rows_out = []
        for (airline, member_srl, proc), wid_set in aircraft_set.items():
            rows_out.append(
                {
                    "airline": airline,
                    "member_srl": int(member_srl),
                    "member_name": member_name_map.get(int(member_srl), f"ID_{member_srl}"),
                    "process": proc,
                    "aircraft_cnt": int(len(wid_set)),
                }
            )

        # 정렬: airline, aircraft_cnt desc
        rows_out.sort(key=lambda r: (r["airline"], -r["aircraft_cnt"], r["member_name"]))

        write_json(
            out_dir,
            "section3_worker_process_counts.json",
            {
                "range": {"from": str(date_from), "to": str(date_to)},
                "rows": rows_out,
                "meta": {
                    "definition": "aircraft_cnt = COUNT(DISTINCT work_id) per (airline, member, process)",
                    "processes": ["소닉", "라바", "로보캅"],
                    "exclude_labels": sorted(list(exclude_labels)),
                    "name_rule": "member_name = nick_name if exists else user_name",
                },
            },
        )

        print("====Section3 ETL 완료")

        # =================================================
        # Section 3-Speed: 작업자별 공정 속도 비교용 RAW JSON
        # - 목적: 같은 airline + work_type + process(+zone)끼리 속도 비교
        # - metric: display_sec = time_sec + backup_sec_attached
        # =================================================

        rows_speed = []

        sql_s3_speed = f"""
        SELECT
          o.airline_code,
          w.work_type,
          w.ex_srl AS work_id,
          wm.wm_srl,
          wm.member_srl,
          wm.total_time AS time_sec,
          0 AS backup_sec,
          d.wdl_label
        FROM rx_air_work_duration_log d
        JOIN rx_air_work_member wm ON wm.wm_srl = d.wm_srl
        JOIN rx_air_work w ON w.ex_srl = wm.work_srl
        JOIN rx_air_operation o ON o.ex_srl = w.operation_srl
        WHERE w.work_type IN ({in_placeholders(len(work_type_ids))})
          AND wm.work_date BETWEEN %s AND %s
          AND o.airline_code IN ({ph})
          AND d.wdl_label NOT IN ('무효','OJT');
        """

        params_speed = (
            sorted(list(work_type_ids))
            + [str(date_from), str(date_to)]
            + airlines
        )

        with conn.cursor() as cur:
            cur.execute(sql_s3_speed, params_speed)
            rows = cur.fetchall()

        # (airline, work_type, work_id, wm_srl) 단위로 라벨 모으기
        labels_by_key = defaultdict(set)
        info_by_key = {}

        for airline, work_type, work_id, wm_srl, member_srl, time_sec, backup_sec, label in rows:
            key = (airline, work_type, work_id, wm_srl)
            if label:
                labels_by_key[key].add(str(label).strip())
            if key not in info_by_key:
                info_by_key[key] = {
                    "member_srl": int(member_srl),
                    "time_sec": int(time_sec or 0),
                    "backup_sec": int(backup_sec or 0),
                }

        # member 이름 매핑 재사용
        member_name_map = {}
        member_ids = sorted(
            list({v["member_srl"] for v in info_by_key.values()})
        )

        if member_ids:
            with conn.cursor() as cur:
                cur.execute(
                    f"""
                    SELECT member_srl, nick_name, user_name
                    FROM rx_member
                    WHERE member_srl IN ({in_placeholders(len(member_ids))});
                    """,
                    member_ids,
                )
                for msrl, nick, uname in cur.fetchall():
                    member_name_map[int(msrl)] = (
                        (nick or "").strip()
                        or (uname or "").strip()
                        or f"ID_{msrl}"
                    )

        # process 분류 함수 재사용
        def pick_process_and_zone(labels: set):
            filtered = {lb for lb in labels if lb and lb not in exclude_labels}
            for lb in filtered:
                for p in sonic_prefixes:
                    if lb.startswith(p):
                        return "소닉", lb
            for lb in filtered:
                for p in lava_prefixes:
                    if lb.startswith(p):
                        return "라바", lb
            for lb in filtered:
                for p in robocop_prefixes:
                    if lb.startswith(p):
                        return "로보캅", lb
            return None, None

        for key, labels in labels_by_key.items():
            airline, work_type, work_id, wm_srl = key
            info = info_by_key[key]

            proc, zone = pick_process_and_zone(labels)
            if not proc:
                continue

            rows_speed.append(
                {
                    "airline": airline,
                    "work_type": work_type,
                    "process": proc,
                    "zone": zone,
                    "member_srl": info["member_srl"],
                    "member_name": member_name_map.get(
                        info["member_srl"], f"ID_{info['member_srl']}"
                    ),
                    "time_sec": info["time_sec"],
                    "backup_sec_attached": info["backup_sec"],
                    "work_id": work_id,
                    "wm_srl": wm_srl,
                }
            )

        import re

        def attach_backup_seconds(rows_speed):
            """
            - 소닉백업존N time_sec 를 같은 work_id의 소닉N 행에 backup_sec_attached로 더함
            - 라바백업 time_sec 를 라바 행에 더함
            - 베큠백업 time_sec 를 베큠 행에 더함
            - 붙인 백업행은 rows에서 제거
            """
            # 메인행을 빠르게 찾기 위한 인덱스: (airline, work_type, work_id, process, zone) -> row
            index = {}
            for r in rows_speed:
                key = (r["airline"], r["work_type"], r["work_id"], r["process"], r["zone"])
                index[key] = r

            out = []
            attached = 0
            unattached = 0

            for r in rows_speed:
                zone = r["zone"] or ""
                is_backup = False

                target_zone = None

                # 1) 소닉백업존N -> 소닉N
                m = re.match(r"^소닉백업존(\d+)$", zone)
                if m:
                    is_backup = True
                    n = m.group(1)
                    target_zone = f"소닉{n}"

                # 2) 라바백업 -> 라바
                if zone == "라바백업":
                    is_backup = True
                    target_zone = "라바"

                # 3) 베큠백업 -> 베큠
                if zone == "베큠백업":
                    is_backup = True
                    target_zone = "베큠"

                if is_backup:
                    target_key = (r["airline"], r["work_type"], r["work_id"], r["process"], target_zone)
                    main_row = index.get(target_key)

                    if main_row:
                        # 백업행의 time_sec를 메인행의 backup_sec_attached에 더함
                        main_row["backup_sec_attached"] = int(main_row.get("backup_sec_attached", 0)) + int(r["time_sec"] or 0)
                        attached += 1
                        # 백업행은 out에 넣지 않음(삭제)
                        continue
                    else:
                        # 붙일 대상이 없으면 (데이터 이상/라벨 누락) 디버깅 위해 남김
                        # 단, 표시를 바꿔서 눈에 띄게
                        r["zone"] = f"{zone}(UNATTACHED)"
                        unattached += 1
                        out.append(r)
                        continue

                # 백업이 아니면 그대로 유지
                out.append(r)

            print(f"[Section3-Speed] backup attached rows={attached}, unattached={unattached}, total={len(rows_speed)} -> {len(out)}")
            return out

        # ✅ 여기서 rows_speed 정리 실행
        rows_speed = attach_backup_seconds(rows_speed)


        write_json(
            out_dir,
            "section3_worker_speed_debug.json",
            {
                "range": {"from": str(date_from), "to": str(date_to)},
                "rows": rows_speed,
                "meta": {
                    "definition": "display_sec = time_sec + backup_sec_attached",
                    "note": "Raw speed rows for worker/process comparison",
                },
            },
        )

        print("====Section3-Speed ETL 완료")



        print("### run_all.py 끝까지 실행됨 ###")

    finally:
        conn.close()


if __name__ == "__main__":
    main()
