// ================================
// Global config / constants
// ================================
/** 항공사 순서 고정(HH/RF/8M) */
const AIRLINE_ORDER = ["HH", "RF", "8M"];

/** 컬러 고정 */
const AIRLINE_COLOR = {
  HH: "#b2c6d3",
  RF: "#69C6DD",
  "8M": "#ECAB86",
};

// ================================
// Utils (shared helpers)
// ================================
async function loadJson(path) {
  const res = await fetch(path);
  if (!res.ok) throw new Error(`Failed to load ${path}`);
  return await res.json();
}

function setDefaultDateToToday() {
  const el = document.getElementById("dateTo");
  if (!el) return;

  const today = new Date();
  const yyyy = today.getFullYear();
  const mm = String(today.getMonth() + 1).padStart(2, "0");
  const dd = String(today.getDate()).padStart(2, "0");
  el.value = `${yyyy}-${mm}-${dd}`;
}

function yyyymmddToISO(yyyymmdd) {
  const s = String(yyyymmdd);
  return `${s.slice(0, 4)}-${s.slice(4, 6)}-${s.slice(6, 8)}`;
}

function fmtMin(sec) {
  return `${(sec / 60).toFixed(1)}분`;
}

function escapeHtml(s) {
  return String(s)
    .replaceAll("&", "&amp;")
    .replaceAll("<", "&lt;")
    .replaceAll(">", "&gt;")
    .replaceAll('"', "&quot;")
    .replaceAll("'", "&#039;");
}

function secToMMSS(sec) {
  if (sec == null || isNaN(sec)) return "-";
  sec = Math.round(sec);
  const m = Math.floor(sec / 60);
  const s = sec % 60;
  return `${m}분 ${String(s).padStart(2, "0")}초`;
}

// ================================
// Section 1-1: 항공사 요약 (전체 조업의 수)
// ================================
function renderDonutCounts(data) {
  // 순서 고정 + 없는 항목은 0으로
  const map = new Map(data.airlines.map((a) => [a.code, a.count]));
  const values = AIRLINE_ORDER.map((code) => Number(map.get(code) ?? 0));
  const colors = AIRLINE_ORDER.map((code) => AIRLINE_COLOR[code] || "#B2C6D3");

  document.getElementById(
    "s1_chart1_title"
  ).textContent = `항공사별 청소건수 (기간: ${data.range.from} ~ ${data.range.to})`;

  const trace = {
    type: "pie",
    labels: AIRLINE_ORDER,
    values,
    marker: { colors },
    hole: 0.55,
    textinfo: "label+percent",
    hovertemplate:
      "%{label}<br>건수: %{value}<br>비율: %{percent}<extra></extra>",
    sort: false,
  };

  const layout = {
    margin: { t: 10, l: 10, r: 10, b: 10 },
    showlegend: true,
    legend: { orientation: "h" },
    paper_bgcolor: "rgba(0,0,0,0)",
    plot_bgcolor: "rgba(0,0,0,0)",
    font: { size: 13, color: "#253036" },
  };

  Plotly.newPlot("s1_chart_counts", [trace], layout, {
    responsive: true,
    displayModeBar: false,
  });
}

// ================================
// Section 1-2: 항공사 요약 (절감 시간)
// ================================
function renderSavedBox(pointsData, statsData) {
  const points = pointsData.points;

  const traces = AIRLINE_ORDER.map((code) => {
    const y = points
      .filter((p) => p.airline === code)
      .map((p) => p.saved_sec / 60); // 분

    return {
      type: "box",
      name: code,
      y,
      marker: { color: AIRLINE_COLOR[code] || "#B2C6D3" },
      line: { color: AIRLINE_COLOR[code] || "#B2C6D3" },
      boxpoints: "all",
      jitter: 0.35,
      pointpos: 0,
      hovertemplate: `${code}<br>절감시간: %{y:.1f}분<extra></extra>`,
    };
  });

  // 요약 문구 순서 고정
  const statsMap = new Map(statsData.stats.map((s) => [s.code, s]));
  const statsLine = AIRLINE_ORDER.filter((code) => statsMap.has(code))
    .map((code) => {
      const s = statsMap.get(code);
      return `${code}: 평균 ${fmtMin(s.avg_saved_sec)} (n=${s.n})`;
    })
    .join(" · ");

  document.getElementById(
    "s1_chart2_title"
  ).textContent = `표준 대비 절감시간 분포 (분) — ${statsLine}`;

  const layout = {
    margin: { t: 10, l: 50, r: 10, b: 40 },
    yaxis: { title: "절감시간(분)", gridcolor: "rgba(0,0,0,.06)" },
    xaxis: { title: "항공사", gridcolor: "rgba(0,0,0,.04)" },
    paper_bgcolor: "rgba(0,0,0,0)",
    plot_bgcolor: "rgba(0,0,0,0)",
    font: { size: 13, color: "#253036" },
  };

  Plotly.newPlot("s1_chart_saved", traces, layout, {
    responsive: true,
    displayModeBar: false,
  });
}

// ================================
// Section 2: 항공기/공정 타임시리즈 탭/카드/레이아웃
// ================================
function renderSection2(listJson, tsJson, procJson) {
  const tabsEl = document.getElementById("s2_tabs");
  const cardsEl = document.getElementById("s2_cards");

  if (!tabsEl || !cardsEl) return;

  const airlines = listJson.airlines || ["HH", "RF", "8M"];
  let active = airlines[0];

  function drawTabs() {
    tabsEl.innerHTML = "";
    airlines.forEach((code) => {
      const btn = document.createElement("button");
      btn.className = "tabBtn" + (code === active ? " active" : "");
      btn.textContent = code;
      btn.onclick = () => {
        active = code;
        drawTabs();
        drawCards();
      };
      tabsEl.appendChild(btn);
    });
  }

  function drawCards() {
    cardsEl.innerHTML = "";

    const aircrafts = listJson.aircraft_by_airline?.[active] || [];
    if (aircrafts.length === 0) {
      cardsEl.innerHTML = `<div class="card"><div class="cardTitle">데이터 없음</div></div>`;
      return;
    }

    aircrafts.forEach((a, idx) => {
      const card = document.createElement("div");
      card.className = "card";

      const title = document.createElement("div");
      title.className = "cardTitle";
      title.textContent = `${active} · 작업타입: ${a.aircraft} (n=${a.n})`;

      card.appendChild(title);

      const div = document.createElement("div");
      const chartId = `s2_chart_${active}_${a.aircraft}_${idx}`.replace(
        /[^a-zA-Z0-9_]/g,
        "_"
      );
      div.id = chartId;
      div.className = "chart";
      card.appendChild(div);

      cardsEl.appendChild(card);

      renderS2AircraftChart(chartId, active, a.aircraft, tsJson); // 2-1 그래프
    });

    // 공정(소닉/라바/로보캅) 그래프 3개
    if (procJson && procJson.series) {
      const processes = ["소닉", "라바", "로보캅"];

      processes.forEach((procName, pIdx) => {
        const key = `${active}|${procName}`;
        const rows = procJson.series?.[key] || [];

        const card = document.createElement("div");
        card.className = "card";

        const title = document.createElement("div");
        title.className = "cardTitle";
        title.textContent = `${active} · 공정: ${procName}`;
        card.appendChild(title);

        const div = document.createElement("div");
        const chartId = `s2_proc_${active}_${procName}_${pIdx}`.replace(
          /[^a-zA-Z0-9_]/g,
          "_"
        );
        div.id = chartId;
        div.className = "chart";
        card.appendChild(div);

        cardsEl.appendChild(card);

        renderS2ProcessChart(chartId, active, procName, procJson); //2-2 그래프
      });
    }
  }

  drawTabs();
  drawCards();
}

// ================================
// Section 2-1: 항공기 작업타입별 그래프
// ================================
function renderS2AircraftChart(elId, airline, aircraft, tsJson) {
  const key = `${airline}|${aircraft}`;
  const rows = tsJson.series?.[key] || [];

  const x = rows.map((r) => yyyymmddToISO(r.yyyymmdd));
  const yBar = rows.map((r) => r.avg_actual_sec / 60); // 분
  const barColors = rows.map((r) =>
    r.avg_actual_sec > r.standard_sec ? "#E05A4F" : "#B2C6D3"
  );

  const stdSec = rows.length ? rows[0].standard_sec : null; // 초(고정)
  const yStd = rows.map(() => (stdSec == null ? null : stdSec / 60)); // 분(선 그리기용)

  const hover = rows.map(
    (r) =>
      `날짜: ${r.yyyymmdd}<br>` +
      `평균: ${secToMMSS(r.avg_actual_sec)}<br>` +
      `최단: ${secToMMSS(r.min_actual_sec)}<br>` +
      `최장: ${secToMMSS(r.max_actual_sec)}<br>` +
      `n=${r.n}<extra></extra>`
  );

  const bar = {
    type: "bar",
    x,
    y: yBar,
    name: "실제(평균)",
    hovertemplate: hover,
    marker: { color: barColors },
  };

  const line = {
    type: "scatter",
    mode: "lines",
    x,
    y: yStd,
    name: "표준(분)",
    line: { color: "#0459A5", width: 2 },
    customdata: rows.map(() => secToMMSS(stdSec)),
    hovertemplate: "표준: %{customdata}<extra></extra>",
  };

  const layout = {
    margin: { t: 10, l: 50, r: 10, b: 40 },
    barmode: "group",
    yaxis: { title: "시간(분)", gridcolor: "rgba(0,0,0,.06)" },
    xaxis: { title: "날짜", gridcolor: "rgba(0,0,0,.04)" },
    paper_bgcolor: "rgba(0,0,0,0)",
    plot_bgcolor: "rgba(0,0,0,0)",
    font: { size: 13, color: "#253036" },
  };

  Plotly.newPlot(elId, [bar, line], layout, {
    responsive: true,
    displayModeBar: false,
  });
}

// ================================
// Section 2-2: 항공기 공정별 그래프
// ================================
function renderS2ProcessChart(elId, airline, processName, procJson) {
  const key = `${airline}|${processName}`;
  const rows = procJson.series?.[key] || [];

  if (rows.length === 0) {
    // 데이터 없으면 빈 그래프 대신 텍스트 처리
    Plotly.newPlot(
      elId,
      [],
      {
        annotations: [
          {
            text: "데이터 없음",
            showarrow: false,
            font: { size: 14 },
          },
        ],
        xaxis: { visible: false },
        yaxis: { visible: false },
        margin: { t: 10, l: 10, r: 10, b: 10 },
        paper_bgcolor: "rgba(0,0,0,0)",
        plot_bgcolor: "rgba(0,0,0,0)",
      },
      { responsive: true, displayModeBar: false }
    );
    return;
  }

  // x: 날짜, y: 1인당 평균(분)
  const x = rows.map((r) => yyyymmddToISO(r.yyyymmdd));
  const y = rows.map((r) => Number(r.avg_min));

  // 최대/최소 색상
  const maxVal = Math.max(...y);
  const minVal = Math.min(...y);
  const colors = y.map((v) => {
    if (v === maxVal) return "#ecab86"; // 빨강
    if (v === minVal) return "#69C6DD"; // 파랑
    return "#B2C6D3"; // 기본 회색
  });

  const hover = rows.map((r) => {
    const avgSec = Number(r.avg_min) * 60;

    return (
      `날짜: ${r.yyyymmdd}<br>` +
      `평균(1인): ${secToMMSS(avgSec)}<br>` +
      `인원: ${r.members}명<br>` +
      `총합: ${secToMMSS(r.sum_sec)}<extra></extra>`
    );
  });

  const bar = {
    type: "bar",
    x,
    y,
    name: "공정 평균(1인)",
    marker: { color: colors },
    hovertemplate: hover,
  };

  // 점선: 기간 평균
  const avg = procJson.period_avg_min?.[key];
  const avgSec = avg * 60;

  const line =
    avg == null
      ? null
      : {
          type: "scatter",
          mode: "lines",
          x,
          y: x.map(() => avg),
          name: "기간 평균",
          line: { dash: "dot", width: 2, color: "#253036" },

          hoverinfo: "skip",
        };

  const annotations =
    avg == null
      ? []
      : [
          {
            x: x[x.length - 1],
            y: avg,
            xanchor: "left",
            yanchor: "middle",
            text: `평균 ${secToMMSS(avgSec)}`,
            showarrow: false,
            font: {
              size: 12,
              color: "#253036",
            },
            bgcolor: "rgba(255,255,255,0.7)",
          },
        ];

  const layout = {
    margin: { t: 10, l: 50, r: 10, b: 40 },
    barmode: "group",
    yaxis: { title: "시간(분)", gridcolor: "rgba(0,0,0,.06)" },
    xaxis: { title: "날짜", gridcolor: "rgba(0,0,0,.04)" },
    paper_bgcolor: "rgba(0,0,0,0)",
    plot_bgcolor: "rgba(0,0,0,0)",
    font: { size: 13, color: "#253036" },
    annotations,
  };

  const traces = line ? [bar, line] : [bar];

  Plotly.newPlot(elId, traces, layout, {
    responsive: true,
    displayModeBar: false,
  });
}

// ================================
// Section 3-1: 작업자별 공정 수행 횟수 (테이블)
// ================================
function renderSection3(data) {
  const tabsEl = document.getElementById("s3_tabs");
  const tableEl = document.getElementById("s3_table");

  if (!tabsEl || !tableEl) return;

  const AIRLINES = ["ALL", "HH", "RF", "8M"];
  let active = "ALL";

  function badgeHtml(proc) {
    if (proc === "소닉") return `<span class="badge sonic">소닉</span>`;
    if (proc === "라바") return `<span class="badge lava">라바</span>`;
    return `<span class="badge robo">로보캅</span>`;
  }

  function drawTabs() {
    tabsEl.innerHTML = "";
    AIRLINES.forEach((code) => {
      const btn = document.createElement("button");
      btn.className = "tabBtn" + (code === active ? " active" : "");
      btn.textContent = code === "ALL" ? "전체" : code;
      btn.onclick = () => {
        active = code;
        drawTabs();
        drawTable();
      };
      tabsEl.appendChild(btn);
    });
  }

  function drawTable() {
    // 1) 항공사 필터
    const baseRows =
      active === "ALL"
        ? data.rows
        : (data.rows || []).filter((r) => r.airline === active);

    if (!baseRows || baseRows.length === 0) {
      tableEl.innerHTML = `<thead><tr><th>데이터 없음</th></tr></thead><tbody></tbody>`;
      return;
    }

    // 2) member 단위로 피벗 집계: { member => {소닉: n, 로보캅: n, 라바: n} }
    const map = new Map();

    baseRows.forEach((r) => {
      const name = r.member_name || String(r.member_srl); // 이름 없으면 srl
      const proc = r.process;
      const cnt = Number(r.aircraft_cnt || 0);

      if (!map.has(name)) {
        map.set(name, {
          member_name: name,
          소닉: 0,
          로보캅: 0,
          라바: 0,
          _total: 0,
        });
      }

      const row = map.get(name);

      // 예상 외 라벨이 들어오면 스킵
      if (proc === "소닉" || proc === "로보캅" || proc === "라바") {
        row[proc] += cnt;
        row._total += cnt;
      }
    });

    // 3) 정렬: 총합(_total) 내림차순
    const finalRows = Array.from(map.values()).sort(
      (a, b) => b._total - a._total
    );

    // 4) 테이블 헤더 (작업자 / 소닉 / 로보캅 / 라바)
    const thead = `
      <thead>
        <tr>
          <th style="width: 40%;">작업자</th>
          <th class="num" style="width: 20%;">${badgeHtml("소닉")}</th>
          <th class="num" style="width: 20%;">${badgeHtml("로보캅")}</th>
          <th class="num" style="width: 20%;">${badgeHtml("라바")}</th>
          <th class="num" style="width: 20%;">합계</th>
        </tr>
      </thead>
    `;

    // 5) 바디
    const tbodyRows = finalRows
      .map((r) => {
        const sonic = r["소닉"] || 0;
        const robo = r["로보캅"] || 0;
        const lava = r["라바"] || 0;
        const total = sonic + robo + lava;
        return `
          <tr>
            <td>${escapeHtml(r.member_name)}</td>
            <td class="num">${sonic.toLocaleString("ko-KR")}</td>
            <td class="num">${robo.toLocaleString("ko-KR")}</td>
            <td class="num">${lava.toLocaleString("ko-KR")}</td>
            <td class="num"><strong>${total.toLocaleString(
              "ko-KR"
            )}</strong></td>
            
          </tr>
        `;
      })
      .join("");

    tableEl.innerHTML = thead + `<tbody>${tbodyRows}</tbody>`;
  }

  drawTabs();
  drawTable();
}

// ================================
// Section 3-2: 공정별 소요시간 순위
// ================================
function renderSection3SpeedChart(data) {
  const chartEl = document.getElementById("s3_speed_charts");
  const airlineTabsEl = document.getElementById("s3_speed_tabs_airline");

  if (!chartEl || !airlineTabsEl) return;

  const AIRLINES = ["HH", "RF", "8M"];
  let activeAirline = AIRLINES[0];

  function drawAirlineTabs() {
    airlineTabsEl.innerHTML = "";
    AIRLINES.forEach((code) => {
      const btn = document.createElement("button");
      btn.className = "tabBtn" + (code === activeAirline ? " active" : "");
      btn.textContent = code;
      btn.onclick = () => {
        activeAirline = code;
        drawAirlineTabs();
        drawCharts();
      };
      airlineTabsEl.appendChild(btn);
    });
  }

  function safeId(s) {
    return String(s).replace(/[^a-zA-Z0-9_]/g, "_");
  }

  // display_sec에서 백업존 시간은 미리 계산되니까 제외 (time + attached backup)
  function calcDisplaySec(r) {
    return Number(r.time_sec || 0);
    // return Number(r.time_sec || 0) + Number(r.backup_sec_attached || 0);
  }

  // 작업자별 평균(편명 1건당 평균) 집계
  function aggregateByMember(rows) {
    const map = new Map(); // key: member_srl or name
    rows.forEach((r) => {
      const key =
        r.member_srl != null
          ? String(r.member_srl)
          : String(r.member_name || "");
      const name = r.member_name || key;

      const displaySec = calcDisplaySec(r);

      if (!map.has(key)) {
        map.set(key, {
          member_key: key,
          member_name: name,
          count: 0,
          sum_display_sec: 0,
          sum_time_sec: 0,
          sample: r,
          // sum_backup_sec는 내부 계산용으로 필요하면 남겨도 되지만,
          // hover에서 안 보여줄 거라면 굳이 안 모아도 됩니다.
        });
      }
      const a = map.get(key);
      a.count += 1;
      a.sum_display_sec += displaySec;
      a.sum_time_sec += Number(r.time_sec || 0);
    });

    const out = Array.from(map.values()).map((a) => ({
      ...a,
      avg_display_sec: a.count ? a.sum_display_sec / a.count : null,
    }));

    // 속도는 빠를수록 좋으니까 오름차순
    out.sort(
      (x, y) => (x.avg_display_sec ?? 1e18) - (y.avg_display_sec ?? 1e18)
    );
    return out;
  }

  function getAirlineFromRow(r) {
    // airline 필드가 없으면 flight_title 앞 2글자 사용
    return r.airline || String(r.flight_title || "").slice(0, 2);
  }

  function drawCharts() {
    chartEl.innerHTML = "";

    // 1) 항공사만 필터 (zone 무시)
    const rows = (data.rows || []).filter(
      (r) => getAirlineFromRow(r) === activeAirline
    );

    if (rows.length === 0) {
      chartEl.innerHTML = `<div class="card" style="min-height:220px;">
        <div class="cardTitle">${activeAirline} — 데이터 없음</div>
        <div class="chart" style="height:160px;display:flex;align-items:center;justify-content:center;color:#63666a;">데이터 없음</div>
      </div>`;
      return;
    }

    // 2) role_label별로 그룹핑 (존/공정 카드 제거)
    const roleMap = new Map(); // key = role_label
    rows.forEach((r) => {
      const role = r.role_label || "(unknown)";
      if (!roleMap.has(role)) roleMap.set(role, []);
      roleMap.get(role).push(r);
    });

    // 3) role_label 카드+차트 생성 (존 없음)
    const roles = Array.from(roleMap.keys())
      .filter((x) => x && x !== "(unknown)")
      .sort();

    // 혹시 role_label이 비어있는 데이터만 있는 경우 대비
    if (roles.length === 0) {
      chartEl.innerHTML = `<div class="card" style="min-height:220px;">
        <div class="cardTitle">${activeAirline} — role_label 데이터 없음</div>
        <div class="chart" style="height:160px;display:flex;align-items:center;justify-content:center;color:#63666a;">데이터 없음</div>
      </div>`;
      return;
    }

    roles.forEach((role, idx) => {
      const rowsInRole = roleMap.get(role) || [];

      const card = document.createElement("div");
      card.className = "card";
      card.style.minHeight = "420px";

      const title = document.createElement("div");
      title.className = "cardTitle";
      title.textContent = `${activeAirline} · ${role}`;
      card.appendChild(title);

      const chartDiv = document.createElement("div");
      const chartId = safeId(`s3_speed_${activeAirline}_${role}_${idx}`);
      chartDiv.id = chartId;
      chartDiv.className = "chart";
      card.appendChild(chartDiv);

      chartEl.appendChild(card);

      // 작업자별 평균으로 1인 1막대
      const agg = aggregateByMember(rowsInRole);

      const y = agg.map((a) => a.member_name);
      const x = agg.map((a) => (a.avg_display_sec ?? 0) / 60); // 분

      // hover에서 backup합 제거
      const hover = agg.map((a) => {
        const avg = a.avg_display_sec ?? 0;
        const s = a.sample || {};
        return (
          `${escapeHtml(a.member_name)}<br>` +
          `샘플: ${escapeHtml(s.date || "")} ${escapeHtml(
            s.flight_title || ""
          )}<br>` +
          `샘플 time_sec: ${Number(s.time_sec || 0)}초<br>` +
          `평균: ${(avg / 60).toFixed(2)}분<br>` +
          `건수: ${a.count}건<br>` +
          `time합: ${secToMMSS(a.sum_time_sec)}<extra></extra>`
        );
      });

      const trace = {
        type: "bar",
        x,
        y,
        orientation: "h",
        hovertemplate: hover,
        marker: { color: "#69C6DD" },
      };

      const layout = {
        margin: { t: 10, l: 90, r: 20, b: 40 },
        xaxis: {
          title: "평균 소요시간(분)  ※ time + attached backup",
          gridcolor: "rgba(0,0,0,.06)",
        },
        yaxis: { automargin: true, autorange: "reversed" },
        paper_bgcolor: "rgba(0,0,0,0)",
        plot_bgcolor: "rgba(0,0,0,0)",
        font: { size: 13, color: "#253036" },
      };

      Plotly.newPlot(chartId, [trace], layout, {
        responsive: true,
        displayModeBar: false,
      });
    });
  }

  drawAirlineTabs();
  drawCharts();
}

// ================================
// Boot / entry
// ================================
async function boot() {
  // 1) UI초기화
  setDefaultDateToToday();

  document.getElementById("btnReload").addEventListener("click", () => {
    alert("기간 선택은 아직 안됨");
  });

  // 2) 데이터 로드
  // let module_path = "./modules/bestturn/skins/new_dashboard/";

  // Section 1
  const counts = await loadJson("data/section1_counts.json");
  const points = await loadJson("data/section1_saved_points.json");
  const stats = await loadJson("data/section1_saved_stats.json");

  // Section2
  const s2List = await loadJson("data/section2_aircraft_list.json");
  const s2Ts = await loadJson("data/section2_aircraft_timeseries.json");
  const s2Proc = await loadJson("data/section2_process_timeseries.json");

  // Section3
  const s3 = await loadJson("data/section3_worker_process_counts.json");
  const s3Speed = await loadJson("data/section3_speed_rows.json");

  // 렌더
  renderDonutCounts(counts); //Section 1-1
  renderSavedBox(points, stats); //Section 1-2
  renderSection2(s2List, s2Ts, s2Proc); //Section2
  renderSection3(s3); //Section 3-1
  renderSection3SpeedChart(s3Speed); //Section 3-2
}

document.addEventListener("DOMContentLoaded", () => {
  boot().catch((err) => {
    console.error(err);
    alert("대시보드 로딩 실패.." + err);
  });
});
